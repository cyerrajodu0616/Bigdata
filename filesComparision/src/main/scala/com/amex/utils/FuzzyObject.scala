package main.scala.com.amex.utils

  import java.io.{BufferedWriter, File, FileWriter}

  import net.liftweb.json.DefaultFormats
  import org.apache.spark.sql.SparkSession

  import scala.collection.mutable.ListBuffer
  import scala.io.Source
  import java.text.SimpleDateFormat
  import java.util.Calendar

  object FuzzyObject {

    def main(args: Array[String]): Unit = {
      if (args.length != 9) {
        println(args.length)
        println("invalid usage of arguments")
        System.exit(1)
      }

      implicit val formats = DefaultFormats

      case class Prop(name:String, bucket:List[String])
      case class Column(column: List[Prop])

      //    val props : Properties = new Properties
      //    props.load(new FileInputStream(args(0)))

      val spark = SparkSession.builder().appName("File Comparision Utility").enableHiveSupport().getOrCreate()
      val sc = spark.sparkContext

      val dataFileName1: String = args(0)
      val dataFileName2: String = args(1)
      val headerFileName: String = args(2)

      val dataDelimiter: String = args(3)
      val headerDelimiter: String = args(4)

      val keyColumn: String = args(5)


      val file = new File( args(6))
      val outFile = new File(args(6) + "/filesComparisonReport.log")

      val propertyFilePercentage = args(7)
      val tableName = args(8)


      /*
      Checking given 2 files are of same size
       */

      val df1Length = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 10).option("inferschema","true").load(dataFileName1).dtypes.length
      val df2Length = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 10).option("inferschema","true").load(dataFileName2).dtypes.length
      val headerLength = spark.read.format("csv").option("delimiter", headerDelimiter).option("inferschema","true").load(headerFileName).dtypes.length

      if (outFile.exists)
        outFile.delete()

      sc.addFile(propertyFilePercentage)

      val bw = new BufferedWriter(new FileWriter(outFile))

      if(df1Length == df2Length){

        if(df1Length == headerLength){

      /*
      Below piece of code is used to find out the bucketing percentage for Numeric Column
      AS of now we will do comparision for required columns in property file with all bucketing percentages.
   */
      val prop = Source.fromFile(propertyFilePercentage)

      val parsedJson = net.liftweb.json.parse(prop.mkString)

      var CoulumnPercent: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap()

      val percnetageBuclket: scala.collection.mutable.TreeSet[Int] = scala.collection.mutable.TreeSet()

      val OutputTable = (parsedJson \ "column").children

      for( x <- OutputTable){
        val Percent = (x \ "bucket").children
        val bucketpercent: StringBuilder = new StringBuilder()
        for(y <-  Percent) {
          bucketpercent.append(y.extract[String]).append(",")
          percnetageBuclket.+=(y.extract[String].toInt)
        }
        CoulumnPercent.+=((x \ "name").extract[String] -> bucketpercent.dropRight(1).toString())
        //ColPerc.+(bucketpercent.dropRight(1).toString())
      }

      var ColNames: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      var columnsReportList: ListBuffer[String] = new ListBuffer()

      val headerSchema = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName").schema
      val arbtHBaseFileDF1 = spark.read.format("csv").option("delimiter", "\t").schema(headerSchema).load(s"$dataFileName1")
      val arbtHBaseFileDF2 = spark.read.format("csv").option("delimiter", "\t").schema(headerSchema).load(s"$dataFileName2")

      arbtHBaseFileDF1.createOrReplaceTempView("arbtHBaseFileDF1View")
      arbtHBaseFileDF2.createOrReplaceTempView("arbtHBaseFileDF2View")


      /*
      Finding out datatype of given data files using infer schema
       */

      val df1 = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 1000).option("inferschema","true").load(dataFileName1).dtypes
      val SchemaList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      df1.foreach(dataType => SchemaList.append(dataType._2))

      val ColumnList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      val headerDf = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName").dtypes
      headerDf.foreach(dataType => ColumnList.append(dataType._1))

      var SchemaDT: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap()
      for(i <- 0 to ColumnList.size-1 )
        SchemaDT.+=(ColumnList(i) -> SchemaList(i))


          if(ColumnList.contains(keyColumn)){


            val Df1Cnt = arbtHBaseFileDF1.select(keyColumn).count()
            val Df2Cnt = arbtHBaseFileDF2.select(keyColumn).count()

            val Df1DistinctCnt = arbtHBaseFileDF1.select(keyColumn).distinct().count()
            val Df2DistinctCnt = arbtHBaseFileDF2.select(keyColumn).distinct().count()


            if(Df1Cnt ==  Df1DistinctCnt && Df2Cnt ==  Df2DistinctCnt) {

              var KeyColumnCondition: String = ""

              /*
            Below code will be used to find out the Key column data type, If Key column is Number then we will add *1 in joining condition.
             */

              if (SchemaDT.get(keyColumn).getOrElse("") != "StringType") {
                KeyColumnCondition = "(a." + keyColumn + "*1 = b." + keyColumn + "*1)"
              } else
                KeyColumnCondition = "(a." + keyColumn + " = b." + keyColumn + ")"

              /*
                Stats for given files
               */

              val File1Only = spark.sql("select a."+keyColumn+" from arbtHBaseFileDF1View a left join arbtHBaseFileDF2View b on " + KeyColumnCondition + " where b."+keyColumn+" is null")
              val File2Only = spark.sql("select a."+keyColumn+" from arbtHBaseFileDF1View a left join arbtHBaseFileDF2View b on " + KeyColumnCondition + " where a."+keyColumn+" is null")

              val matchCount = spark.sql("select a."+keyColumn+" from arbtHBaseFileDF1View a join arbtHBaseFileDF2View b on " + KeyColumnCondition )

              if ( matchCount.count() > 0) {


                var createStmnt: StringBuilder = new StringBuilder()

                val minuteFormat = new SimpleDateFormat("YYYYMMddHHmmSSS")
                val currentTimestamp = minuteFormat.format(Calendar.getInstance.getTime)

                val TableName:String = "FileComparision.filecomaprision"+currentTimestamp

                spark.sql("drop table if exists " + tableName)

                createStmnt = createStmnt.append("create table if not exists ").append(tableName).append(" as select ")

                val conditionStmnt: StringBuilder = new StringBuilder()

                var ConditionBucketList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
                var BucketList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()

                ColumnList.map(x => {
                  if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
                    val bucketPercentage = percnetageBuclket.mkString(",")
                    for (buc <- bucketPercentage.split(",", -1)) {

                      var perc = ""
                      if (buc.length == 1)
                        perc = "1.0" + buc
                      else
                        perc = "1." + buc


/*                      val BucketConditionString: String = " sum( case " +
                        s"when a.$x is not null and a.$x <= $buc and b.$x is null then 1 " +
                        s"when a.$x is not null and b.$x is not null and (ABS(a.$x - b.$x) <= 1 or a.$x = b.$x) then 1 " +
                        s"when b.$x >= 1 and (ABS(round(b.$x,6) - round(a.$x,6))/round(b.$x,6))*100 <= $buc then 1 " +
                        s"when ((a.$x >= 0 or a.$x <= $buc) and b.$x = 0) or (a.$x is null and b.$x = 0 ) then 1 else 0 end ) as ${x}_${buc}_Stat,"


                      val BucketCountConditionString: String = " count( case " +
                        s"when a.$x is not null and a.$x <= $buc and b.$x is null then 1 " +
                        s"when a.$x is not null and b.$x is not null and (ABS(a.$x - b.$x) <= 1 or a.$x = b.$x) then 1 " +
                        s"when b.$x >= 1 and (ABS(b.$x - a.$x)/b.$x)*100 <= $buc then 1 " +
                        s"when (a.$x >= 0 and a.$x <= $buc and b.$x = 0) or (a.$x is null and b.$x = 0 ) then 1 " +
                        s"when a.$x != b.$x then 0 else null end ) as ${x}_${buc}_cnt,"*/

                      val BucketConditionString: String =  "sum( case "+
                      s"when ABS(b.$x) < 1  or ABS(b.$x) > ${buc} and round(ABS((round(b.$x,4) - round(a.$x,4))/round(b.$x,4)),4) <= 0.${buc} then 1 "+
                      s"when ABS(b.$x) < 1  or ABS(b.$x) > ${buc} and b.$x is not null  and a.$x is NOT null and b.$x != 0 then 0 "+
                      s"when (ABS(b.$x) >= 1 or ABS(b.$x) <= ${buc}) and round(ABS(round(b.$x,4) - round(a.$x,4)),4) <= 1 then 1 " +
                      s"when (ABS(b.$x) >= 1 or ABS(b.$x) <= ${buc}) and b.$x is not null  and a.$x is not null then 0 else null end ) as ${x}_${buc}_Stat,"

                      val BucketCountConditionString: String =  "count(CASE "+
                        s"when ABS(b.$x) < 1 or ABS(b.$x) > ${buc} and round(ABS((round(b.$x,4) - round(a.$x,4))/round(b.$x,4)),4) <= 0.${buc} then 1 "+
                        s"when ABS(b.$x) < 1  or ABS(b.$x) > ${buc} and b.$x is not null  and a.$x is not null and b.$x != 0 then 0 "+
                        s"when (ABS(b.$x) >= 1 or ABS(b.$x) <= ${buc}) and ABS(round(b.$x,4) - round(a.$x,4)) <= 1 then 1 " +
                        s"when (ABS(b.$x) >= 1 or ABS(b.$x) <= ${buc}) and b.$x is not null  and a.$x is not null then 0 " +
                        s"else null end ) as ${x}_${buc}_cnt,"

                      ConditionBucketList.+=(BucketConditionString)
                      ConditionBucketList.+=(BucketCountConditionString)
                      ColNames.+=(x + "_" + buc + "_Stat")
                      BucketList.+=(x)
                    }
                  }
                   ConditionBucketList.+=(s"sum(case when a.$x = b.$x then 1 when a.$x is not null and b.$x is not null and  a.$x != b.$x then 0 else NULL end) as ${x}_Stat,")
                   ConditionBucketList.+=(s"count(case when a.$x = b.$x then 1 " +
                     s"when a.$x is not null and b.$x is not null and  a.$x != b.$x then 0 " +
                     s"else NULL end) as  ${x}_Cnt,")

                  ConditionBucketList.+=(s"count(case when a.$x is not null then 1 else null end) as ${x}_f1_Only,")
                  ConditionBucketList.+=(s"count(case when b.$x is not null then 1 else null end) as ${x}_f2_Only,")


                  ColNames.+=(x + "_Stat")
                })

                //val conditionList: scala.collection.mutable.ListBuffer[String] = ColumnList.map(x => "sum(case when nvl(a." + x + ",0) = nvl(b." + x + ",0) then 1 else 0 end) as " + x + "_Stat,")


                ConditionBucketList.foreach(dataType => conditionStmnt.append(dataType))
                conditionStmnt.deleteCharAt(conditionStmnt.length - 1)

                val fromStmnt: String = " from arbtHBaseFileDF1View a join arbtHBaseFileDF2View b on " + KeyColumnCondition
                createStmnt = createStmnt.append(conditionStmnt).append(fromStmnt)

                val stmnt: String = createStmnt.toString

                //println(stmnt)

                val statsDf = spark.sql(s"$stmnt")

                println("Job successfully completed")
                bw.close


              }
              else {
                bw.write("There is no matched data in given files based on Key Column")
                println("There is no matched data in given files based on Key Column")
                bw.close
                System.exit(1)

              }

            }
            else {
              bw.write(s"Given Key column( $keyColumn) is not unique in data file, Please check and resubmit")
              println(s"Given Key column( $keyColumn) is not unique in data file, Please check and resubmit")
              bw.close
              System.exit(1)

            }
          }else
          {
            bw.write(s"Given Key column( $keyColumn) is not present in provided Header file ($headerFileName), Please check and resubmit")
            println(s"Given Key column( $keyColumn) is not present in provided Header file ($headerFileName), Please check and resubmit")
            bw.close
            System.exit(1)

          }
        }else{

          bw.write(s"Given Header and data files are not having same number of fields, Please check and resubmit")
          println(s"Given Header and data files are not having same number of fields, Please check and resubmit")
          bw.close
          System.exit(1)

        }

      }else{

        bw.write(s"Given Data files are not having same number of fields, Please check and resubmit")
        println(s"Given Data files are not having same number of fields, Please check and resubmit")
        bw.close
        System.exit(1)

      }
    }


}
