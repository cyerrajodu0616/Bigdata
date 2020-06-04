package main.scala.com.amex.utils

  import java.io.{BufferedWriter, File, FileWriter}

  import net.liftweb.json.DefaultFormats
  import org.apache.spark.sql.SparkSession

  import scala.collection.mutable.ListBuffer
  import scala.io.Source
  import java.text.SimpleDateFormat
  import java.util.Calendar

  import org.apache.commons.io.FileUtils

  object FuzzyCompJson {

    def main(args: Array[String]): Unit = {

      try {

        if (args.length != 1) {
          println(args.length)
          println("invalid usage of arguments")
          System.exit(1)
        }


        implicit val formats = DefaultFormats

        case class Prop(name: String, bucket: List[String])
        case class Column(column: List[Prop])

        val minuteFormat = new SimpleDateFormat("YYYYMMddHHmmSSS")
        val currentTimestamp = minuteFormat.format(Calendar.getInstance.getTime)

        //val tableName: String = "FileComparision.filecomaprision" + currentTimestamp + "_a"

        val tr = true

        val propertyFilePercentage = args(0)
        val prop = Source.fromFile(propertyFilePercentage)
        val parsedJson = net.liftweb.json.parse(prop.mkString)


        val dataFileName1: String = (parsedJson \ "datafile1").extract[String]
        val dataFileName2: String = (parsedJson \ "datafile2").extract[String]
        val headerFileName: String = (parsedJson \ "headerFile").extract[String]

        val dataDelimiter: String = (parsedJson \ "dataDelimiter").extract[String]
        val headerDelimiter: String = (parsedJson \ "headerDelimiter").extract[String]

        val keyColumn: String = (parsedJson \ "keyColumn").extract[String]


        val file = new File((parsedJson \ "outLocation").extract[String])
        val outFile = new File(file + "/filesComparisonReport.log")

        val convertedFile1 = (parsedJson \ "outLocation").extract[String] + "/dataf1"
        val convertedFile2 = (parsedJson \ "outLocation").extract[String] + "/dataf2"


        val FeedOutFile = new File(file + "/filesComparison.csv")

        if(FeedOutFile.exists())
          FeedOutFile.delete()

        if (outFile.exists)
          outFile.delete()

        val bw = new BufferedWriter(new FileWriter(outFile))
        val fbw = new BufferedWriter(new FileWriter(FeedOutFile))


        try {

          //val tableName = (parsedJson\ "tableName").extract[String]

          val queueName = (parsedJson \ "queueName").extract[String]


          val spark = SparkSession.builder().appName("File Comparision Utility").config("spark.yarn.queue", queueName).enableHiveSupport().getOrCreate()


          /*
      Checking given 2 files are of same size
       */

          val df1Length = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 10).load(dataFileName1).dtypes.length
          val df2Length = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 10).load(dataFileName2).dtypes.length
          val headerLength = spark.read.format("csv").option("delimiter", headerDelimiter).load(headerFileName).dtypes.length


          if (df1Length == df2Length) {

            if (df1Length == headerLength) {

              /*
          Below piece of code is used to find out the bucketing percentage for Numeric Column
          AS of now we will do comparision for required columns in property file with all bucketing percentages.
       */

              var CoulumnPercent: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap()

              val percnetageBuclket: scala.collection.mutable.TreeSet[Int] = scala.collection.mutable.TreeSet()

              val OutputTable = (parsedJson \ "column").children

              for (x <- OutputTable) {
                val Percent = (x \ "bucket").children
                val bucketpercent: StringBuilder = new StringBuilder()
                for (y <- Percent) {
                  bucketpercent.append(y.extract[String]).append(",")
                  percnetageBuclket.+=(y.extract[String].toInt)
                }
                CoulumnPercent.+=((x \ "name").extract[String] -> bucketpercent.dropRight(1).toString())
              }


              var ColNames: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
              var columnsReportList: ListBuffer[String] = new ListBuffer()


              val headerSchema = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName").schema


              spark.sparkContext.textFile(dataFileName1).map(x => x.replaceAll(s"\\\\N", s"")).saveAsTextFile(convertedFile1)
              spark.sparkContext.textFile(dataFileName2).map(x => x.replaceAll(s"\\\\N", s"")).saveAsTextFile(convertedFile2)


              val arbtHBaseFileDF1 = spark.read.format("csv").option("delimiter", dataDelimiter).schema(headerSchema).load(s"$convertedFile1")
              val arbtHBaseFileDF2 = spark.read.format("csv").option("delimiter", dataDelimiter).schema(headerSchema).load(s"$convertedFile2")

              arbtHBaseFileDF1.createOrReplaceTempView("arbtHBaseFileDF1View")
              arbtHBaseFileDF2.createOrReplaceTempView("arbtHBaseFileDF2View")


              /*
          Finding out datatype of given data files using infer schema
           */

              val df1 = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 1000).option("inferschema", "true").load(convertedFile1).dtypes
              val SchemaList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
              df1.foreach(dataType => SchemaList.append(dataType._2))

              val ColumnList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
              val headerDf = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName").dtypes
              headerDf.foreach(dataType => ColumnList.append(dataType._1))

              var SchemaDT: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap()
              for (i <- 0 to ColumnList.size - 1)
                SchemaDT.+=(ColumnList(i) -> SchemaList(i))


              val RequiredColumns = (parsedJson \ "requiredColum").children

              val RequiredColumnList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
              if (RequiredColumns.size != 0) {
                for (z <- RequiredColumns)
                  RequiredColumnList.+=(z.extract[String])
              }


              if (ColumnList.contains(keyColumn)) {

                val df1 = spark.sql(s"select count(${keyColumn}) as a_cnt, count(distinct(${keyColumn})) as a_dist_Cnt from arbtHBaseFileDF1View")
                val df2 = spark.sql(s"select count(${keyColumn}) as a_cnt, count(distinct(${keyColumn})) as a_dist_Cnt from arbtHBaseFileDF2View")

                val Df1Cnt_df = df1.select("a_cnt").collect.map(_ (0))
                val Df1disCnt_df = df1.select("a_dist_Cnt").collect.map(_ (0))

                val Df2Cnt_df = df2.select("a_cnt").collect.map(_ (0))
                val Df2disCnt_df = df2.select("a_cnt").collect.map(_ (0))

                val Df1Cnt = Df1Cnt_df(0).toString
                val Df1DistinctCnt = Df1disCnt_df(0).toString

                val Df2Cnt = Df2Cnt_df(0).toString
                val Df2DistinctCnt = Df2disCnt_df(0).toString


                if (Df1Cnt == Df1DistinctCnt && Df2Cnt == Df2DistinctCnt) {

                  var KeyColumnCondition: String = ""

                  println(" Key Column Data type " + SchemaDT.get(keyColumn).getOrElse(""))

                  /*
            Below code will be used to find out the Key column data type, If Key column is Number then we will add *1 in joining condition.
             */

                  if (SchemaDT.get(keyColumn).getOrElse("") != "StringType") {
                    KeyColumnCondition = "(a." + keyColumn + "*1 = b." + keyColumn + "*1)"
                  } else
                    KeyColumnCondition = "(a." + keyColumn + " = b." + keyColumn + ")"

                  println(s" Condition string : ${KeyColumnCondition}")

                  val matchCount = spark.sql("select a." + keyColumn + " from arbtHBaseFileDF1View a join arbtHBaseFileDF2View b on " + KeyColumnCondition )

                  println("Matched count " + matchCount.count())

                  if (matchCount.count() > 0) {

                    val file1Count = Df1Cnt
                    val file2Count = Df2Cnt
                    val MatchedCount =  matchCount.count()
                    val diff = BigDecimal(BigInt(MatchedCount)/BigInt(Df1Cnt))*100

                    val stats = Df1Cnt + "," +  Df2Cnt + "," + MatchedCount + "," + diff

                    fbw.write(stats)

                    fbw.close()


                    var createStmnt: StringBuilder = new StringBuilder()

                    val minuteFormat = new SimpleDateFormat("YYYYMMddHHmmSSS")
                    val currentTimestamp = minuteFormat.format(Calendar.getInstance.getTime)

                    val dbName = "tempDb" + currentTimestamp
                    val TableName: String = s"${dbName}.filecomaprision" + currentTimestamp

                    val dbLocation = (parsedJson \ "outLocation").extract[String] + "/tempdb" + currentTimestamp


                    println(dbName)
                    spark.sql("drop table if exists " + TableName)

                    createStmnt = createStmnt.append("create table if not exists ").append(TableName).append(" as select ")

                    val conditionStmnt: StringBuilder = new StringBuilder()

                    var ConditionBucketList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
                    var BucketList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()

                    if (RequiredColumnList.size == 0) {
                      ColumnList.map(x => {
                        if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
                          if (CoulumnPercent.contains(x)) {
                            val bucketPercentage = percnetageBuclket.mkString(",")
                            for (buc <- bucketPercentage.split(",", -1)) {

                              val percentage = if (buc.toInt<10) "0"+buc.toInt else buc.toInt


                              val BucketConditionString: String = "sum( case " +
                                s"when (ABS(round(b.$x,6)) < 1  or ABS(round(b.$x,6)) > 10)  and ABS((round(b.$x,6) - round(a.$x,6))/round(b.$x,6)) <= 0.$percentage THEN 1 " +
                                s"WHEN round(b.$x,6)*100000000 = 0 and round(a.$x,6)*100000000 = 0 THEN 1 " +
                                s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <= 10 and round(ABS(round(b.$x,6) - round(a.$x,6)),5) <= 1 THEN 1 " +
                                s"WHEN ( ABS(round(b.$x,6)) < 1  or ABS(round(b.$x,6)) > 10 ) and b.$x is NOT NULL  and round(a.$x,6) is NOT NULL THEN 0 " +
                                s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <= 10 and round(b.$x,6) is NOT NULL and round(a.$x,6) is NOT NULL THEN 0 else null end ) as ${x}_${buc}_Stat,"

                              val BucketCountConditionString: String = "count( CASE " +
                                s"when (ABS(round(b.$x,6)) < 1  or ABS(round(round(b.$x,6),6)) >  10)  and ABS((round(b.$x,6) - round(a.$x,6))/round(b.$x,6)) <= 0.$percentage THEN 1 " +
                                s"WHEN round(b.$x,6)*100000000 = 0 and round(a.$x,6)*100000000 = 0 THEN 1 " +
                                s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <=  10 and round(ABS(round(b.$x,6) - round(a.$x,6)),5) <= 1 THEN 1 " +
                                s"WHEN ( ABS(round(b.$x,6)) < 1  or ABS(round(b.$x,6)) >  10 ) and round(b.$x,6) is NOT NULL and round(a.$x,6) is NOT NULL THEN 0 " +
                                s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <=  10 and round(b.$x,6) is NOT NULL  and round(a.$x,6) is NOT NULL THEN 0 " +
                                s"else null end ) as ${x}_${buc}_cnt,"

                              ConditionBucketList.+=(BucketConditionString)
                              ConditionBucketList.+=(BucketCountConditionString)
                              ColNames.+=(x + "_" + buc + "_Stat")
                              BucketList.+=(x)
                            }
                          }
                        }

                        if (SchemaDT.get(x).getOrElse("") != "StringType") {
                          ConditionBucketList.+=(s"sum(case when a.$x is not null and b.$x is not null and round(a.$x*1,6) = round(b.$x*1,6) then 1 else NULL end) as ${x}_Stat,")
                        } else {
                          ConditionBucketList.+=(s"sum(case when (a.$x is not null and b.$x is not null) and a.$x = b.$x then 1 else NULL end) as ${x}_Stat,")
                        }

                        /*ConditionBucketList.+=(s"count(case when a.$x = b.$x then 1 " +
                          s"when a.$x is not null and b.$x is not null and  a.$x != b.$x then 0 " +
                          s"else NULL end) as  ${x}_Cnt,")*/

                        ConditionBucketList.+=(s"count(case when a.$x is not null and b.$x is not null then 0 " +
                          s"else NULL end) as  ${x}_Cnt,")

                        ConditionBucketList.+=(s"count(case when a.$x is not null then 1 else null end) as ${x}_f1_Only,")
                        ConditionBucketList.+=(s"count(case when b.$x is not null then 1 else null end) as ${x}_f2_Only,")


                        ColNames.+=(x + "_Stat")
                      })
                    } else {
                      RequiredColumnList.map(x => {
                        if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
                          val bucketPercentage = percnetageBuclket.mkString(",")
                          for (buc <- bucketPercentage.split(",", -1)) {

                            val percentage = if (buc.toInt<10) "0"+buc.toInt else buc.toInt


                            val BucketConditionString: String = "sum( case " +
                              s"when (ABS(round(b.$x,6)) < 1  or ABS(round(b.$x,6)) > 10)  and ABS((round(b.$x,6) - round(a.$x,6))/round(b.$x,6)) <= 0.${percentage} THEN 1 " +
                              s"WHEN round(b.$x,6)*100000000 = 0 and round(a.$x,6)*100000000 = 0 THEN 1 " +
                              s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <= 10 and round(ABS(round(b.$x,6) - round(a.$x,6)),5) <= 1 THEN 1 " +
                              s"WHEN ( ABS(round(b.$x,6)) < 1  or ABS(round(b.$x,6)) > 10 ) and b.$x is NOT NULL  and round(a.$x,6) is NOT NULL THEN 0 " +
                              s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <= 10 and round(b.$x,6) is NOT NULL and round(a.$x,6) is NOT NULL THEN 0 else null end ) as ${x}_${buc}_Stat,"

                            val BucketCountConditionString: String = "count( CASE " +
                              s"when (ABS(round(b.$x,6)) < 1  or ABS(round(round(b.$x,6),6)) >  10)  and round(ABS((round(b.$x,6) - round(a.$x,6))/round(b.$x,6)),5) <= 0.${percentage} THEN 1 " +
                              s"WHEN round(b.$x,6)*100000000 = 0 and round(a.$x,6)*100000000 = 0 THEN 1 " +
                              s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <=  10 and round(ABS(round(b.$x,6) - round(a.$x,6)),5) <= 1 THEN 1 " +
                              s"WHEN ( ABS(round(b.$x,6)) < 1  or ABS(round(b.$x,6)) >  10 ) and round(b.$x,6) is NOT NULL and round(a.$x,6) is NOT NULL THEN 0 " +
                              s"WHEN ABS(round(b.$x,6)) >= 1 and ABS(round(b.$x,6)) <=  10 and round(b.$x,6) is NOT NULL  and round(a.$x,6) is NOT NULL THEN 0 " +
                              s"else null end ) as ${x}_${buc}_cnt,"

                            ConditionBucketList.+=(BucketConditionString)
                            ConditionBucketList.+=(BucketCountConditionString)
                            ColNames.+=(x + "_" + buc + "_Stat")
                            BucketList.+=(x)
                          }
                        }

                        if (SchemaDT.get(x).getOrElse("") != "StringType") {
                          ConditionBucketList.+=(s"sum(case when (a.$x is not null and b.$x is not null) and round(a.$x*1,6) = round(b.$x*1,6) then 1 else NULL end) as ${x}_Stat,")
                        } else {
                          ConditionBucketList.+=(s"sum(case when (a.$x is not null and b.$x is not null) and a.$x = b.$x then 1 else NULL end) as ${x}_Stat,")
                        }

                        ConditionBucketList.+=(s"count(case when a.$x is not null and b.$x is not null then 0 " +
                          s"else NULL end) as  ${x}_Cnt,")

                        ConditionBucketList.+=(s"count(case when a.$x is not null then 1 else null end) as ${x}_f1_Only,")
                        ConditionBucketList.+=(s"count(case when b.$x is not null then 1 else null end) as ${x}_f2_Only,")
                        ColNames.+=(x + "_Stat")
                      })

                    }


                    ConditionBucketList.foreach(dataType => conditionStmnt.append(dataType))
                    conditionStmnt.deleteCharAt(conditionStmnt.length - 1)

                    val fromStmnt: String = " from arbtHBaseFileDF1View a join arbtHBaseFileDF2View b on " + KeyColumnCondition
                    createStmnt = createStmnt.append(conditionStmnt).append(fromStmnt)

                    val stmnt: String = createStmnt.toString

                    spark.sql(s"create database if not exists $dbName location '$dbLocation'")

                    println(stmnt)

                    spark.sql(s"$stmnt")

                    bw.write(s"$TableName")
                    println("Job successfully completed")
                    bw.close()


                  }
                  else {
                    bw.write("There is no matched data in given files based on Key Column")
                    println("There is no matched data in given files based on Key Column")
                    bw.close()
                    System.exit(1)

                  }

                }
                else {
                  bw.write(s"Given Key column( $keyColumn) is not unique in data file, Please check and resubmit")
                  println(s"Given Key column( $keyColumn) is not unique in data file, Please check and resubmit")
                  bw.close()
                  System.exit(1)

                }
              } else {
                bw.write(s"Given Key column( $keyColumn) is not present in provided Header file ($headerFileName), Please check and resubmit")
                println(s"Given Key column( $keyColumn) is not present in provided Header file ($headerFileName), Please check and resubmit")
                bw.close()
                System.exit(1)

              }
            } else {

              bw.write(s"Given Header and data files are not having same number of fields, Please check and resubmit")
              println(s"Given Header and data files are not having same number of fields, Please check and resubmit")
              bw.close()
              System.exit(1)

            }

          } else {

            bw.write(s"Given Data files are not having same number of fields, Please check and resubmit")
            println(s"Given Data files are not having same number of fields, Please check and resubmit")
            bw.close()
            System.exit(1)

          }

        }
        catch {
          case e: Exception => e.printStackTrace()
            bw.write(e.printStackTrace().toString)
            FileUtils.deleteDirectory(new File(convertedFile1))
            FileUtils.deleteDirectory(new File(convertedFile2))
            bw.close()
            System.exit(1)

        }
      }catch {
        case e: Exception => e.printStackTrace()
          System.exit(1)
      }
    }

  }
