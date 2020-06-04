package main.scala.com.amex.utils


import java.io.{BufferedWriter, File, FileWriter}
import net.liftweb.json.DefaultFormats
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.Source

object WritingFile {
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
    val headerFileName: String = args(1)
    val dataDelimiter: String = args(2)
    val headerDelimiter: String = args(3)
    val keyColumn: String = args(4)
    val propertyFilePercentage = args(5)
    val file = new File( args(6))
    val tableName = args(7)

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

    /*
    Checking given 2 files are of same size
     */

    val statTableDf = spark.read.table(tableName).cache()

    ColumnList.foreach(x => {
      println("Column "+ x)
      val clmn = x
      val Columnvalue = statTableDf.select(x+"_Stat").collect.map(_(0))
      val matchCnt = Columnvalue(0).toString()

      val CntValue = statTableDf.select(x+"_Cnt").collect.map(_(0))
      val matchedCount = Columnvalue(0).toString()

      val matchPercentage = (BigDecimal(matchCnt)/BigDecimal(matchedCount))*100
      val unmatchedCnt = BigDecimal(matchedCount) - BigDecimal(matchCnt)
      val unmatchedPercentage = 100 - matchPercentage

      var statMsg:StringBuilder = new StringBuilder()

      statMsg.append(clmn + "," + matchedCount + "," + matchCnt + "," + matchPercentage + "%," + unmatchedCnt + "," + unmatchedPercentage +"%")

      println(statMsg.toString())

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
          val BucketColumnvalue = statTableDf.select(columnName).collect.map(_(0))
          println(BucketColumnvalue(0).toString)
          val bucketMatchCnt = BucketColumnvalue(0).toString()
          val bucketMatchPercentage = (BigDecimal(bucketMatchCnt)/BigDecimal(matchedCount))*100
          statMsg.append( "," + bucketMatchCnt + "," + bucketMatchPercentage + "%,")
        }
        statMsg.append(",")
      }
      val Stat:String = statMsg.toString().dropRight(1)
      columnsReportList.+=(Stat)
      println("Stat "+Stat)

    })

    bw.write(columnsStatsHeader + "\n")
    columnsReportList.foreach(f => {
      bw.write(f + "\n")
    })

    bw.close()

  }

}
