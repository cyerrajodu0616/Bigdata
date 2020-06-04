package main.scala.com.amex.utils

import java.io._

import net.liftweb.json.DefaultFormats
import org.apache.commons.io.FileUtils
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.Source

object FuzzyCompWriteJsonDifferentHeader {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("invalid usage of arguments")
      System.exit(1)
    }

    implicit val formats = DefaultFormats

    case class Prop(name: String, bucket: List[String])
    case class Column(column: List[Prop])
    case class ImportanceProp(name: String, value: String)

    val propertyFilePercentage = args(0)
    val prop = Source.fromFile(propertyFilePercentage)
    val parsedJson = net.liftweb.json.parse(prop.mkString)

    val headerFileName1: String = (parsedJson \ "headerFile1").extract[String]
    val headerFileName2: String = (parsedJson \ "headerFile2").extract[String]

    val dataDelimiter: String = (parsedJson \ "dataDelimiter").extract[String]
    val headerDelimiter: String = (parsedJson \ "headerDelimiter").extract[String]

    val file = new File((parsedJson \ "outLocation").extract[String])


    val file1Label = (parsedJson \ "file1Label").extract[String]
    val file2Label = (parsedJson \ "file2Label").extract[String]

    //val tableName = (parsedJson\ "tableName").extract[String]

    val outFile = new File(file + "/filesComparisonReport.csv")
    val queueName = (parsedJson \ "queueName").extract[String]


    val logFile = new File(file + "/filesComparisonReport.log")
    val br = new BufferedReader(new FileReader(logFile))

    val tableName = br.readLine()

    println(tableName)

    val dbname = tableName.split("\\.")(0)

    if (outFile.exists)
      outFile.delete()

    if(new File(file + "/filesComparisonReportRowWise.xls").exists)
      new File(file + "/filesComparisonReportRowWise.xls").delete()

    val outFile1 = new File(file + "/filesComparisonReportRowWise.csv")

    if (outFile1.exists)
      outFile1.delete()


    val bw = new BufferedWriter(new FileWriter(outFile))
    val outList: ListBuffer[Array[String]] = new ListBuffer[Array[String]]


    val convertedFile1 = (parsedJson \ "outLocation").extract[String] + "/dataf1"
    val convertedFile2 = (parsedJson \ "outLocation").extract[String] + "/dataf2"
    val spark = SparkSession.builder().appName("File Comparision Utility").config("spark.yarn.queue", queueName).enableHiveSupport().getOrCreate()

    try {

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

      val RequiredColumns = (parsedJson \ "requiredColum").children

      val RequiredColumnList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      if (RequiredColumns.nonEmpty) {
        for (z <- RequiredColumns)
          RequiredColumnList.+=(z.extract[String])
      }


      var coulumnImportance: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap()
      val importanceList = (parsedJson \ "importance").children
      importanceList.map(x => coulumnImportance.+=((x \ "name").extract[String] -> (x \ "value").extract[String]))

      val columnType = (parsedJson \ "columnType").children
      val colTypeHashMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      columnType.map(x => (x \ "cols").children.map(y => colTypeHashMap += (y.extract[String] -> (x \ "type").extract[String])))

      colTypeHashMap.foreach(println(_))

      var ColNames: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      var columnsReportList: ListBuffer[String] = new ListBuffer()

      val header1 = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName1").columns
      val header2 = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName2").columns

      val header1Col = header1.map(x => x.toLowerCase).toList
      val header2Col = header2.map(x => x.toLowerCase()).toList
      val CommonCol1 = header1Col.intersect(header2Col)

      /*
    Finding out datatype of given data files using infer schema
     */

      val df1 = spark.read.format("csv").option("delimiter", dataDelimiter).option("sampleSize", 1000).option("inferschema", "true").load(convertedFile1).dtypes
      val SchemaList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      df1.foreach(dataType => SchemaList.append(dataType._2))

      val ColumnList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      val headerDf = spark.read.format("csv").option("header", true).option("delimiter", headerDelimiter).load(s"$headerFileName1").dtypes
      headerDf.foreach(dataType => ColumnList.append(dataType._1))

      var SchemaDT: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap()

      for (i <- 0 to ColumnList.size - 1)
        SchemaDT.+=(ColumnList(i) -> SchemaList(i))
      var BucketList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()

      ColumnList.foreach(x => {
        if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
          BucketList.+=(x)
        }
      })


      var columnStatHeader: StringBuilder = new StringBuilder()

      columnStatHeader.append("Field,Total Count,Matched Count,Matched Percentage,Unmatched Count,Unmatched Percentage,")

      if (BucketList.size > 0) {
        percnetageBuclket.foreach(x => {
          columnStatHeader.append(x.toString.concat("% Match Count,").concat(x.toString).concat("% Match Percentage,"))
        })
      }

      val columnsStatsHeader: String = columnStatHeader.toString().dropRight(1)

      val header: StringBuilder = new StringBuilder().append("Column Name,")
      val file1Only: StringBuilder = new StringBuilder().append(s"${file1Label} Only Count,")
      val file2Only: StringBuilder = new StringBuilder().append(s"${file2Label} Only Count,")
      val sumInd: StringBuilder = new StringBuilder().append("Matched Count,")
      val countInd: StringBuilder = new StringBuilder().append("Not Null Count,")
      val percentageInd: StringBuilder = new StringBuilder().append("Percentage Match,")

      val importance: StringBuilder = new StringBuilder().append("Importance,")
      val colType: StringBuilder = new StringBuilder().append("ColType,")

      val percMatch: StringBuilder = new StringBuilder()

      /*
    Checking given 2 files are of same size
     */

      val ColumnList1 = ColumnList.intersect(CommonCol1)
      val RequiredColumnListCommon = RequiredColumnList.intersect(RequiredColumnList)

      val statTableDf = spark.read.table(tableName).cache()
      //val matchCount = statTableDf.selectExpr(s"""nvl(${keyColumn}_Stat,0)""").collect.map(_(0))
      //val matchedCount = matchCount(0).toString

      if (statTableDf.count() > 0) {
        if (RequiredColumnListCommon.isEmpty) {
          ColumnList1.foreach(x => {
            println("Column " + x)
            val clmn = x
            //val Columnvalue = statTableDf.select(x+"_Stat").collect.map(_(0))

            val Columnvalue = statTableDf.selectExpr(s"""nvl(${x}_Stat,0)""").collect().map(_ (0))

            val matchCnt = Columnvalue(0).toString()


            val TotalColumnValue = statTableDf.selectExpr(s"""nvl(${x}_Cnt,0)""").collect.map(_ (0))
            val totalCnt = TotalColumnValue(0).toString

            val f1OnlyValue = statTableDf.selectExpr(s""" nvl(${x}_f1_Only,0)""").collect.map(_ (0))
            val f1Count = f1OnlyValue(0).toString

            val f2OnlyValue = statTableDf.selectExpr(s""" nvl(${x}_f2_Only,0)""").collect.map(_ (0))
            val f2Count = f2OnlyValue(0).toString

            var matchPercentage: BigDecimal = BigDecimal(0)

            if (!totalCnt.equalsIgnoreCase("0"))
              matchPercentage = (BigDecimal(matchCnt) / BigDecimal(totalCnt)) * 100
            else
              matchPercentage = BigDecimal(0)


            var statMsg: StringBuilder = new StringBuilder()

            val impString = coulumnImportance.getOrElse(x, "N/A")

            colType.append(colTypeHashMap.getOrElse(x, "N/A")+",")
            importance.append(s"${impString},")
            header.append(s"$clmn,")
            file1Only.append(s"$f1Count,")
            file2Only.append(s"$f2Count,")
            sumInd.append(s"$matchCnt,")
            countInd.append(s"$totalCnt,")
            percentageInd.append(s"${matchPercentage}%,")

          })
        } else {
          RequiredColumnListCommon.foreach(x => {
            val clmn = x
            val Columnvalue = statTableDf.selectExpr(s"""nvl(${x}_Stat,0)""").collect.map(_ (0))
            val matchCnt = Columnvalue(0).toString()


            val TotalColumnValue = statTableDf.selectExpr(s"""nvl(${x}_Cnt,0)""").collect.map(_ (0))
            val totalCnt = TotalColumnValue(0).toString

            val f1OnlyValue = statTableDf.selectExpr(s""" nvl(${x}_f1_Only,0)""").collect.map(_ (0))
            val f1Count = f1OnlyValue(0).toString

            val f2OnlyValue = statTableDf.selectExpr(s""" nvl(${x}_f2_Only,0)""").collect.map(_ (0))
            val f2Count = f2OnlyValue(0).toString

            var matchPercentage: BigDecimal = BigDecimal(0)

            if (!totalCnt.equalsIgnoreCase("0"))
              matchPercentage = (BigDecimal(matchCnt) / BigDecimal(totalCnt)) * 100
            else
              matchPercentage = BigDecimal(0)

            var statMsg: StringBuilder = new StringBuilder()

            val impString = coulumnImportance.getOrElse(x, "N/A")

            colType.append(colTypeHashMap.getOrElse(x, "N/A")+",")
            importance.append(s"${impString},")
            header.append(s"$clmn,")
            file1Only.append(s"$f1Count,")
            file2Only.append(s"$f2Count,")
            sumInd.append(s"$matchCnt,")
            countInd.append(s"$totalCnt,")
            percentageInd.append(s"${matchPercentage}%,")

          })
        }


        val Header: String = header.toString().dropRight(1)
        val FileOnly: String = file1Only.toString().dropRight(1)
        val File2Only: String = file2Only.toString().dropRight(1)
        val CountInd: String = countInd.toString().dropRight(1)
        val SumInd: String = sumInd.toString().dropRight(1)
        val PrecentageMatch: String = percentageInd.toString().dropRight(1)
        val columnImpString: String = importance.toString().dropRight(1)
        val colTypeString: String = colType.toString().dropRight(1)

        val emptyString = Header.split(",").map(s => " ,").mkString

        outList += Header.split(",")
        outList += columnImpString.split(",")
        outList += colTypeString.split(",")
        outList += FileOnly.split(",")
        outList += File2Only.split(",")
        outList += CountInd.split(",")
        outList += SumInd.split(",")
        outList += PrecentageMatch.split(",")


        outList += emptyString.split(",")
        outList += emptyString.split(",")

        //      bw.write(Header + "\n")
        //      bw.write(FileOnly + "\n")
        //      bw.write(File2Only + "\n")
        //      bw.write(CountInd + "\n")
        //      bw.write(SumInd + "\n")
        //      bw.write(PrecentageMatch + "\n")


        //      bw.write("\n")
        //      bw.write("\n")


        /*
    Processing bucket stats
     */

        percnetageBuclket.foreach(y => {
          val statMsg: StringBuilder = new StringBuilder()
          val statMsgPerc: StringBuilder = new StringBuilder()
          val statMsgCnt: StringBuilder = new StringBuilder()

          statMsgCnt.append(s"${y}% Count,")
          statMsg.append(s"${y}% Delta Match,")
          statMsgPerc.append(s"${y}% Delta Match Percentage,")

          if (RequiredColumnList.size == 0) {
            ColumnList1.foreach(x => {
              val clmn = x
              if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
                val columnValue = statTableDf.selectExpr(s"nvl(${x}_${y}_Stat,0)").collect.map(_ (0))
                val matchCnt = columnValue(0).toString

                val TotalColumnValue = statTableDf.selectExpr(s"nvl(${x}_${y}_Cnt,0)").collect.map(_ (0))
                val totalCnt = TotalColumnValue(0).toString

                var bucketMatchPercentage: BigDecimal = BigDecimal(0)

                if (!totalCnt.equalsIgnoreCase("0"))
                  bucketMatchPercentage = (BigDecimal(matchCnt) / BigDecimal(totalCnt)) * 100
                else
                  bucketMatchPercentage = BigDecimal(0)

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
          } else {
            RequiredColumnList.foreach(x => {
              val clmn = x
              if (CoulumnPercent.contains(x) && SchemaDT.get(x).getOrElse("") != "StringType") {
                val columnValue = statTableDf.selectExpr(s"nvl(${x}_${y}_Stat,0)").collect.map(_ (0))
                val matchCnt = columnValue(0).toString

                val TotalColumnValue = statTableDf.selectExpr(s"nvl(${x}_${y}_Cnt,0)").collect.map(_ (0))
                val totalCnt = TotalColumnValue(0).toString

                var bucketMatchPercentage: BigDecimal = BigDecimal(0)

                if (!totalCnt.equalsIgnoreCase("0"))
                  bucketMatchPercentage = (BigDecimal(matchCnt) / BigDecimal(totalCnt)) * 100
                else
                  bucketMatchPercentage = BigDecimal(0)
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

          println(StatMsgCnt)
          println(StatMsg)
          println(StatPerc)

          outList += StatMsgCnt.split(",")
          outList += StatMsg.split(",")
          outList += StatPerc.split(",")

          outList += emptyString.split(",")
          outList += emptyString.split(",")
          //        bw.write(s"${StatMsgCnt}\n")
          //        bw.write(s"${StatMsg}\n")
          //        bw.write(s"${statMsgPerc}\n")
          //
          //        bw.write("\n")
          //        bw.write("\n")


        })





        val bw1 = new BufferedWriter(new FileWriter(outFile1))
        var i = 1;
        outList.foreach(array => {
          println(array)
          println(s"Fields count at row $i is ${array.length}")
          i += 1
          bw1.write(array.mkString(","))
          bw1.write("\n")
        })
        bw1.close()


        val outListTransposed = outList.transpose
        outListTransposed.foreach(array => {
          bw.write(array.mkString(","))
          bw.write("\n")
        })
        bw.close()

        val cf = new ConditionalFormat();
        val hwb = new HSSFWorkbook()

        val rowCount: Int = cf.createExcelWorkbook(hwb, outListTransposed)
        val conditionalWB = cf.getConditionalWorkbook(hwb, rowCount)



        import java.io.FileOutputStream
        val outFile2 = new FileOutputStream(file + "/filesComparisonReport.xls")
        conditionalWB.write(outFile2)



      }
    }finally {
      FileUtils.deleteDirectory(new File(convertedFile1))
      FileUtils.deleteDirectory(new File(convertedFile2))
      spark.sql(s"drop database if exists $dbname cascade")

    }
  }

}
