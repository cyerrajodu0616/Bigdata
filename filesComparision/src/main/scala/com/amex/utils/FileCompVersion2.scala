package main.scala.com.amex.utils

  import java.io.{BufferedWriter, File, FileWriter}
  import net.liftweb.json.DefaultFormats
  import org.apache.spark.sql.SparkSession

  import scala.collection.mutable.ListBuffer
  import scala.io.Source


  import java.text.SimpleDateFormat
  import java.util.Calendar

  object FileCompVersion2 {

    def main(args: Array[String]): Unit = {
      if (args.length != 8) {
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
      val headerFileName: String = args(3)

      val headerDelimiter: String = args(4)
      val dataDelimiter: String = args(2)
      val keyColumn: String = args(5)

      val propertyFilePercentage = args(7)

      val outFile = new File(args(6) + "/filesComparisonReport.csv")

      if (outFile.exists)
        outFile.delete()

      val bw = new BufferedWriter(new FileWriter(outFile))

      /*
      Below piece of code is used to find out the bucketing percentage for Numeric Column
      AS of now we will do comparision for required columns in property file with all bucketing percentages.
   */
      val prop = Source.fromFile(propertyFilePercentage)

      val parsedJson = net.liftweb.json.parse(prop.mkString)

      var CoulumnPercent: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap()

      //var ColPerc: scala.collection.mutable.HashSet[String] = scala.collection.mutable.HashSet()

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

      val fileLine1 = "File Level comparison report"
      //val fileHeader1 = "File1,File2,File1-File2,File2-File1,Match Count,Match Percentage,UnMatch Count,Unmatch Percentage"
      val fileHeader1 = "File1,File2,File1-File2,File2-File1,Match Count,File 1 Match Percentage,File 2 Match Percentage,File 1 UnMatch Count,File 1 UnMatch Percentage,File 2 UnMatch Count,File 2 UnMatch Percentage"



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


      /*
      Checking given 2 files are of same size
       */

      val df1Length = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 10).option("inferschema","true").load(dataFileName1).dtypes.length
      val df2Length = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 10).option("inferschema","true").load(dataFileName2).dtypes.length
      val headerLength = spark.read.format("csv").option("delimiter", headerDelimiter).option("inferschema","true").load(headerFileName).dtypes.length


      if(df1Length == df2Length){

        if(df1Length == headerLength){

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
                var createStmnt1: StringBuilder = new StringBuilder()

                val minuteFormat = new SimpleDateFormat("YYYYMMddHHmmSSS")
                val currentTimestamp = minuteFormat.format(Calendar.getInstance.getTime)

                val TableName:String = "FileComparision.filecomaprision"+currentTimestamp

                createStmnt = createStmnt.append("create table if not exists ").append(TableName).append(" as select ")

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

                      val BucketConditionString: String = "sum( case when (ABS(((nvl(b."+x+",0) - nvl(a."+x+",0))/nvl(a."+x+",1))*100) < "+buc+".1) or ( a."+x+" = b."+x+") or (length(b."+x+") = 0 AND length(a."+x+") = 0) then 1 else 0 end ) as " + x + "_" + buc + "_Stat,"
                      ConditionBucketList.+=(BucketConditionString)
                      ColNames.+=(x + "_" + buc + "_Stat")
                      BucketList.+=(x)
                    }
                  }
                  ConditionBucketList.+=("sum(case when nvl(a." + x + ",0) = nvl(b." + x + ",0) then 1 else 0 end) as " + x + "_Stat,")
                  ColNames.+=(x + "_Stat")
                })

                val conditionList: scala.collection.mutable.ListBuffer[String] = ColumnList.map(x => "sum(case when nvl(a." + x + ",0) = nvl(b." + x + ",0) then 1 else 0 end) as " + x + "_Stat,")


                ConditionBucketList.foreach(dataType => conditionStmnt.append(dataType))
                conditionStmnt.deleteCharAt(conditionStmnt.length - 1)

                val fromStmnt: String = " from arbtHBaseFileDF1View a join arbtHBaseFileDF2View b on " + KeyColumnCondition
                createStmnt = createStmnt.append(conditionStmnt).append(fromStmnt)

                createStmnt1 = createStmnt1.append(conditionStmnt).append(fromStmnt)
                val stmnt1: String = createStmnt1.toString


                val stmnt: String = createStmnt.toString

                println(stmnt)

                val statsDf = spark.sql(s"$stmnt")

                val stattableDf = spark.read.table(TableName).cache()

                val onlyData1 = File1Only.count()
                val onlyData2 = File2Only.count()

                val F1unmatched = Df1Cnt - matchCount.count()
                val F1matchPercentage = (matchCount.count().toFloat/Df1Cnt.toFloat)*100
                val F1unmatchedPercentage = 100 - F1matchPercentage


                val F2unmatched = Df2Cnt - matchCount.count()
                val F2matchPercentage = (matchCount.count().toFloat/Df2Cnt.toFloat)*100
                val F2unmatchedPercentage = 100 - F2matchPercentage

                var fileOutStats = ""
                fileOutStats = Df1Cnt + "," + Df2Cnt + "," + onlyData1 + "," + onlyData2 + "," + matchCount.count() + "," + F1matchPercentage + "," + F2matchPercentage + ","+ F1unmatched + "," + F1unmatchedPercentage + "," + F2unmatched + "," + F2unmatchedPercentage

                var columnStatHeader: StringBuilder = new StringBuilder()

                columnStatHeader.append("Field,Total Count,Matched Count,Matched Percentage,Unmatched Count,Unmatched Percentage,")

                if(BucketList.size > 0) {
                  percnetageBuclket.foreach(x => {
                    columnStatHeader.append(x.toString.concat("% Match Count,").concat(x.toString).concat("% Match Percentage,"))
                  })
                }

                val columnsStatsHeader:String = columnStatHeader.toString().dropRight(1)



                ColumnList.foreach(x => {
                  println("Column "+ x)
                  val clmn = x
                  val Columnvalue = stattableDf.select(x+"_Stat").collect.map(_(0))
                  val matchCnt = Columnvalue(0).toString()
                  val matchPercentage = (BigDecimal(matchCnt)/BigDecimal(matchCount.count()))*100
                  val unmatchedCnt = BigDecimal(matchCount.count()) - BigDecimal(matchCnt)
                  val unmatchedPercentage = 100 - matchPercentage

                  var statMsg:StringBuilder = new StringBuilder()

                  statMsg.append(clmn + "," + matchCount.count() + "," + matchCnt + "," + matchPercentage + "%," + unmatchedCnt + "," + unmatchedPercentage +"%")

                  if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
                    val bucketPercentage = percnetageBuclket.mkString(",")
                    for (buc <- bucketPercentage.split(",", -1)) {
                      var perc = ""
                      if (buc.length == 1)
                        perc = "1.0" + buc
                      else
                        perc = "1." + buc

                      val columnName = x + "_" + buc + "_Stat"
                      println(columnName)
                      val BucketColumnvalue = stattableDf.select(columnName).collect.map(_(0))
                      println(BucketColumnvalue(0).toString)
                      val bucketMatchCnt = BucketColumnvalue(0).toString()
                      val bucketMatchPercentage = (BigDecimal(bucketMatchCnt)/BigDecimal(matchCount.count()))*100
                      //val bucketUnmatchedCnt = BigDecimal(matchCount.count()) - BigDecimal(bucketMatchCnt)
                      //val bucketUnmatchedPercentage = 100 - bucketMatchPercentage
                      statMsg.append( "," + bucketMatchCnt + "," + bucketMatchPercentage + "%,")
                    }
                    statMsg.append(",")
                  }
                  else
                  {

                    val fileCompHeaderCnt = columnsStatsHeader.split(",").length
                    val ststmsgHeaderCnt = statMsg.toString().split(",").length

                    for (i <- 0 to fileCompHeaderCnt-ststmsgHeaderCnt)
                      statMsg.append(",")

                  }
                  val Stat:String = statMsg.toString().dropRight(1)
                  columnsReportList.+=(Stat)

                  println("Stat "+Stat)

                })

                bw.write(fileLine1 + "\n")
                bw.write(fileHeader1 + "\n")
                bw.write(fileOutStats + "\n")
                bw.newLine
                bw.write(columnsStatsHeader + "\n")
                columnsReportList.foreach(f => {
                  bw.write(f + "\n")
                })
                if (onlyData1 > 0) {
                  bw.write("\n" + "Keys from file1 only \n")
                  File1Only.limit(10).collect().foreach(f => {
                    bw.write(f + "\n")
                  })
                }
                if (onlyData2 > 0) {
                  bw.write("\n" + "Keys from file2 only \n")
                  File1Only.limit(10).collect().foreach(f => {
                    bw.write(f + "\n")
                  })
                }
                bw.close

                println("Job successfully completed")

              }
              else {
                bw.write(fileLine1 + "\n")
                bw.write(fileHeader1 + "\n")
                bw.write("There is no matched data in given files based on Key Column")
                println("There is no matched data in given files based on Key Column")
                bw.close
              }

            }
            else {
              bw.write(fileLine1 + "\n")
              bw.write(fileHeader1 + "\n")
              bw.write(s"Given Key column( $keyColumn) is not unique in data file, Please check and resubmit")
              println(s"Given Key column( $keyColumn) is not unique in data file, Please check and resubmit")
              bw.close
            }
          }else
          {
            bw.write(fileLine1 + "\n")
            bw.write(fileHeader1 + "\n")
            bw.write(s"Given Key column( $keyColumn) is not present in provided Header file ($headerFileName), Please check and resubmit")
            println(s"Given Key column( $keyColumn) is not present in provided Header file ($headerFileName), Please check and resubmit")
            bw.close
          }

        }else{

          bw.write(fileLine1 + "\n")
          bw.write(fileHeader1 + "\n")
          bw.write(s"Given Header and data files are not having same number of fields, Please check and resubmit")
          println(s"Given Header and data files are not having same number of fields, Please check and resubmit")
          bw.close

        }

      }else{

        bw.write(fileLine1 + "\n")
        bw.write(fileHeader1 + "\n")
        bw.write(s"Given Data files are not having same number of fields, Please check and resubmit")
        println(s"Given Data files are not having same number of fields, Please check and resubmit")
        bw.close

      }

    }

  }
