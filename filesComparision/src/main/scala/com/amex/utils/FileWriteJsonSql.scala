package main.scala.com.amex.utils

  import java.io._

  import net.liftweb.json.DefaultFormats
  import org.apache.spark.sql.SparkSession

  import scala.collection.mutable.ListBuffer
  import scala.io.Source

  object FileWriteJsonSql {
    def main(args: Array[String]): Unit = {
      if (args.length != 1) {
        println("invalid usage of arguments")
        System.exit(1)
      }

      implicit val formats = DefaultFormats

      case class Prop(name:String, bucket:List[String])
      case class Column(column: List[Prop])

      val propertyFilePercentage = args(0)
      val prop = Source.fromFile(propertyFilePercentage)
      val parsedJson = net.liftweb.json.parse(prop.mkString)


      //    val props : Properties = new Properties
      //    props.load(new FileInputStream(args(0)))


      val dataFileName1: String = (parsedJson\ "datafile1").extract[String]
      val headerFileName: String = (parsedJson\ "headerFile").extract[String]
      val dataDelimiter: String = (parsedJson\ "dataDelimiter").extract[String]
      val headerDelimiter: String = (parsedJson\ "headerDelimiter").extract[String]
      val keyColumn: String = (parsedJson\ "keyColumn").extract[String]
      val file = new File( (parsedJson\ "outLocation").extract[String])
      val tableName = (parsedJson\ "tableName").extract[String]

      val outFile = new File(file + "/filesComparisonReport.csv")
      val queueName = (parsedJson\ "queueName").extract[String]


      if (outFile.exists)
        outFile.delete()

      val bw = new BufferedWriter(new FileWriter(outFile))


      val spark = SparkSession.builder().appName("File Comparision Utility").config("spark.yarn.queue",queueName).enableHiveSupport().getOrCreate()



      /*
      Below piece of code is used to find out the bucketing percentage for Numeric Column
      AS of now we will do comparision for required columns in property file with all bucketing percentages.
   */

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
      }

      val RequiredColumns = (parsedJson \ "requiredColum").children

      val RequiredColumnList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      if(RequiredColumns.size != 0 )
      {
        for( z <- RequiredColumns)
          RequiredColumnList.+=(z.extract[String])
      }

      var ColNames: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      var columnsReportList: ListBuffer[String] = new ListBuffer()

      val headerSchema = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName").schema


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

      var BucketList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()

      ColumnList.foreach(x => {
        if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
          BucketList.+=(x)
        }
      })


      var columnStatHeader: StringBuilder = new StringBuilder()

      columnStatHeader.append("Field,Total Count,Matched Count,Matched Percentage,Unmatched Count,Unmatched Percentage,")

      if(BucketList.size > 0) {
        percnetageBuclket.foreach(x => {
          columnStatHeader.append(x.toString.concat("% Match Count,").concat(x.toString).concat("% Match Percentage,"))
        })
      }

      val columnsStatsHeader:String = columnStatHeader.toString().dropRight(1)

      val header:StringBuilder = new StringBuilder().append("Column Name,")
      val file1Only:StringBuilder = new StringBuilder().append("File1 Only Count,")
      val file2Only:StringBuilder = new StringBuilder().append("File2 Only Count,")
      val sumInd:StringBuilder = new StringBuilder().append("Matched Count,")
      val countInd:StringBuilder = new StringBuilder().append("Not Null Count,")
      val percentageInd:StringBuilder = new StringBuilder().append("Percentage Match,")

      val percMatch:StringBuilder = new StringBuilder()

      /*
      Checking given 2 files are of same size
       */

      val statTableDf = spark.read.table(tableName).cache()
      val matchCount = statTableDf.select(keyColumn+"_Stat").collect.map(_(0))
      val matchedCount = matchCount(0).toString

      if(RequiredColumnList.size == 0 ){
        ColumnList.foreach(x => {
          println("Column "+ x)
          val clmn = x
          val Columnvalue = statTableDf.select(x+"_Stat").collect.map(_(0))
          val matchCnt = Columnvalue(0).toString()


          val TotalColumnValue = statTableDf.select(x+"_Cnt").collect.map(_(0))
          val totalCnt = TotalColumnValue(0).toString

          val f1OnlyValue = statTableDf.select(x+"_f1_Only").collect.map(_(0))
          val f1Count = f1OnlyValue(0).toString

          val f2OnlyValue = statTableDf.select(x+"_f2_Only").collect.map(_(0))
          val f2Count = f2OnlyValue(0).toString

          val matchPercentage = (BigDecimal(matchCnt)/BigDecimal(totalCnt))*100

          var statMsg:StringBuilder = new StringBuilder()

          header.append(s"$clmn,")
          file1Only.append(s"$f1Count,")
          file2Only.append(s"$f2Count,")
          sumInd.append(s"$matchCnt,")
          countInd.append(s"$totalCnt,")
          percentageInd.append(s"${matchPercentage}%,")

        })
      }else{
        RequiredColumnList.foreach(x => {
          println("Column "+ x)
          val clmn = x
          val Columnvalue = statTableDf.select(x+"_Stat").collect.map(_(0))
          val matchCnt = Columnvalue(0).toString()


          val TotalColumnValue = statTableDf.select(x+"_Cnt").collect.map(_(0))
          val totalCnt = TotalColumnValue(0).toString

          val f1OnlyValue = statTableDf.select(x+"_f1_Only").collect.map(_(0))
          val f1Count = f1OnlyValue(0).toString

          val f2OnlyValue = statTableDf.select(x+"_f2_Only").collect.map(_(0))
          val f2Count = f2OnlyValue(0).toString

          val matchPercentage = (BigDecimal(matchCnt)/BigDecimal(totalCnt))*100

          var statMsg:StringBuilder = new StringBuilder()

          header.append(s"$clmn,")
          file1Only.append(s"$f1Count,")
          file2Only.append(s"$f2Count,")
          sumInd.append(s"$matchCnt,")
          countInd.append(s"$totalCnt,")
          percentageInd.append(s"${matchPercentage}%,")

        })
      }




      val Header:String = header.toString().dropRight(1)
      val FileOnly:String = file1Only.toString().dropRight(1)
      val File2Only:String = file2Only.toString().dropRight(1)
      val CountInd:String = countInd.toString().dropRight(1)
      val SumInd:String = sumInd.toString().dropRight(1)
      val PrecentageMatch:String = percentageInd.toString().dropRight(1)

      bw.write(Header + "\n")
      bw.write(FileOnly + "\n")
      bw.write(File2Only + "\n")
      bw.write(CountInd + "\n")
      bw.write(SumInd + "\n")
      bw.write(PrecentageMatch + "\n")


      bw.write("\n")
      bw.write("\n")


      /*
      Processing bucket stats
       */

      percnetageBuclket.foreach(y => {
        val statMsg:StringBuilder = new StringBuilder()
        val statMsgPerc:StringBuilder = new StringBuilder()
        val statMsgCnt:StringBuilder = new StringBuilder()

        statMsgCnt.append(s"${y}% Count,")
        statMsg.append(s"${y}% Delta Match,")
        statMsgPerc.append(s"${y}% Delta Match Percentage,")

        if(RequiredColumnList.size == 0) {
          ColumnList.foreach(x => {
            val clmn = x
            if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
              val columnValue = statTableDf.select(s"${x}_${y}_Stat").collect.map(_ (0))
              val matchCnt = columnValue(0).toString

              val TotalColumnValue = statTableDf.select(s"${x}_${y}_Cnt").collect.map(_ (0))
              val totalCnt = TotalColumnValue(0).toString

              val bucketMatchPercentage = (BigDecimal(matchCnt) / BigDecimal(totalCnt)) * 100

              statMsgCnt.append(s"${totalCnt},")
              statMsg.append(s"${matchCnt},")
              statMsgPerc.append(s"${bucketMatchPercentage.toDouble}%,")
            }
            else {
              statMsgCnt.append("N/A,")
              statMsg.append("N/A,")
              statMsgPerc.append("N/A,")
            }
          })
        }else{
          RequiredColumnList.foreach(x => {
            val clmn = x
            if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
              val columnValue = statTableDf.select(s"${x}_${y}_Stat").collect.map(_ (0))
              val matchCnt = columnValue(0).toString

              val TotalColumnValue = statTableDf.select(s"${x}_${y}_Cnt").collect.map(_ (0))
              val totalCnt = TotalColumnValue(0).toString

              val bucketMatchPercentage = (BigDecimal(matchCnt) / BigDecimal(totalCnt)) * 100

              statMsgCnt.append(s"${totalCnt},")
              statMsg.append(s"${matchCnt},")
              statMsgPerc.append(s"${bucketMatchPercentage.toDouble}%,")
            }
            else {
              statMsgCnt.append("N/A,")
              statMsg.append("N/A,")
              statMsgPerc.append("N/A,")
            }
          })
        }

        val StatMsgCnt = statMsgCnt.toString().dropRight(1)
        val StatMsg = statMsg.toString().dropRight(1)
        val StatPerc = statMsgPerc.toString().dropRight(1)


        bw.write(s"${StatMsgCnt}\n")
        bw.write(s"${StatMsg}\n")
        bw.write(s"${statMsgPerc}\n")

        bw.write("\n")
        bw.write("\n")


      })

      bw.close()

    }

  }
