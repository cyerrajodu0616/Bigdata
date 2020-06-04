package com.amex.utils

import java.io._

import net.liftweb.json.DefaultFormats
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source

object FilesComparisonUtility {


  def createCombiner = (f: (String, String)) => {
    var sampleList: List[String] = List()
    if (StringUtils.isNotBlank(f._2))
      sampleList = sampleList :+ f._2
    var mc: Int = 0
    var uc: Int = 0
    if (f._1.equals("1"))
      mc = 1
    else
      uc = 1
    (mc, uc, 1, sampleList)
    //match_flag,cnt,sample
  }

  def mergeValue = (acc: (Int, Int, Int, List[String]), value: (String, String)) => {
    var sampleList = acc._4
    if (sampleList.size < 5 && StringUtils.isNotBlank(value._2)) {
      sampleList = sampleList :+ value._2
    }
    var mc: Int = 0
    var uc: Int = 0
    if (value._1.equals("1"))
      mc = 1
    else
      uc = 1

    (acc._1 + mc, acc._2 + uc, acc._3 + 1, sampleList)
  }

  def mergeCombiner = (acc1: (Int, Int, Int, List[String]), acc2: (Int, Int, Int, List[String])) => {
    var sampleList1 = acc1._4
    val sampleList2 = acc2._4
    var i: Int = 0
    while (sampleList1.size < 5 && i < sampleList2.size) {
      sampleList1 = sampleList1 :+ sampleList2(i)
      i = i + 1
    }
    (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3, sampleList1)


  }

  def main(args: Array[String]): Unit = {

    if (args.length != 8) {
      println("invalid usage of arguments")
      System.exit(1)
    }

    implicit val formats = DefaultFormats


    case class Column(column: List[Prop])
    case class Prop(name:String, bucket:List[String])

    //    val props : Properties = new Properties
    //    props.load(new FileInputStream(args(0)))

    val spark = SparkSession.builder().appName("File Comparision Utility").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val dataFileName1: String = args(0)
    val dataFileName2: String = args(1)
    val headerFileName: String = args(3)

    val headerDelimiter: String = args(4)
    val dataDelimiter: String = args(2)
    val keyColumn: String = args(5).toLowerCase

    val propertyFilePercentage = args(7)

    val prop = Source.fromFile(propertyFilePercentage)

    val parsedJson = net.liftweb.json.parse(prop.mkString)

    var CoulumnPercent: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap()

    var ColPerc: scala.collection.mutable.HashSet[String] = scala.collection.mutable.HashSet()

    val OutputTable = (parsedJson \ "column").children

    for( x <- OutputTable){
      val Percent = (x \ "bucket").children
      val bucketpercent: StringBuilder = new StringBuilder()
      for(y <-  Percent)
        bucketpercent.append(y.extract[String]).append(",")
      CoulumnPercent.+=((x \ "name").extract[String] -> bucketpercent.dropRight(1).toString())
      ColPerc.+(bucketpercent.dropRight(1).toString())
    }

    // Finding out data type of columns using builtin infer schema option
    val df1 = spark.read.format("csv").option("delimiter", dataDelimiter).option("inferschema","true").load(dataFileName1).dtypes
    var SchemaList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
    df1.foreach(dataType => SchemaList.append(dataType._2))


    // Reading Header File
    val br = new BufferedReader(new FileReader(new File(headerFileName)))
    val header = br.readLine.toLowerCase

    //val headerLength = header.length
    val headerArray: Array[String] = header.split(headerDelimiter, -1)
    br.close

    val fieldPosition = new HashMap[String, Int]
    for (i <- 0 until headerArray.length)
      fieldPosition(headerArray(i)) = i

    val outColumnsSet = fieldPosition.keySet.-(keyColumn)



    val data1 = sc.textFile(dataFileName1, 20).map(line => {
      val lineSplit = line.split(dataDelimiter, -1)
      val keyColumnData = lineSplit(fieldPosition(keyColumn))
      (keyColumnData, lineSplit)
    })
    data1.cache
    val data1Count = data1.count()
    println("data 1 count is " + data1Count)
    val data2 = sc.textFile(dataFileName2, 20).map(line => {
      val lineSplit = line.split(dataDelimiter, -1)
      val keyColumnData = lineSplit(fieldPosition(keyColumn))
      (keyColumnData, lineSplit)
    })
    data2.cache

    val data2Count = data2.count()
    println("data 2 count is " + data2Count)

    val d1d2 = data1.keys.subtract(data2.keys)
    val d2d1 = data2.keys.subtract(data1.keys)

    val  data = data1.join(data2)

    val union_count = data1.fullOuterJoin(data2)

    data.cache()
    val intersection_count = data.count()
    println("intersection count is " + intersection_count)

    var fileStats: (String, String) = ("", "")
    var columnsReportList: ListBuffer[(String, String)] = new ListBuffer


    if(data.count() > 0){

      // Finding DataType for given columns
      val df1 = spark
        .read
        .format("csv")
        .option("delimiter", dataDelimiter)
        .option("inferschema","true")
        .load(dataFileName1)
        .dtypes


      var SchemaList: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      df1.foreach(dataType => SchemaList.append(dataType._2))

      val dataSplit = data.values.map(valueTuples => {

        val dataArray1 = valueTuples._1
        val dataArray2 = valueTuples._2

        val keyData: String = dataArray1(fieldPosition(keyColumn))

        var outList: List[String] = List()
        var keyMatched: Boolean = true
        for (field <- outColumnsSet) {
          var matchFlag: Int = 0
          var outData: String = ""
          if (dataArray1(fieldPosition(field)) == dataArray2(fieldPosition(field))) {
            matchFlag = 1
            val outRecord = fieldPosition(field) + "~@~@~" + field + "~~" + matchFlag + "~@~@~" + outData
            outList = outList :+ outRecord
          }
          else {
            keyMatched = false
            outData = keyData + " | " + dataArray1(fieldPosition(field)) + " | " + dataArray2(fieldPosition(field))
            val outRecord = fieldPosition(field) + "~@~@~" + field + "~~" + matchFlag + "~@~@~" + outData
            outList = outList :+ outRecord
          }

          //Additional Calculations




        }
        if (keyMatched)
          outList = outList :+ "-1~@~@~Records~~1~@~@~"
        else
          outList = outList :+ "-1~@~@~Records~~0~@~@~" + keyData

        outList

      }).flatMap(x => x).map(f => {
        val values = f.split("~~", -1)(1).split("~@~@~", -1)
        (f.split("~~", -1)(0), (values(0), values(1)))
      })

      val combinerRDD = dataSplit.combineByKey(createCombiner, mergeValue, mergeCombiner)

      val outRDD = combinerRDD.map(f => {
        val outFieldPosition = Integer.parseInt(f._1.split("~@~@~", -1)(0))
        val outField = f._1.split("~@~@~", -1)(1)
        val values = f._2

        val match_percentage = ((values._1.toFloat / values._3.toFloat) * 100).toFloat
        val unmatch_percentage = ((values._2.toFloat / values._3.toFloat) * 100).toFloat


        val sb = new StringBuffer
        for (i <- 1 to 5) {
          if (i <= values._4.size) {
            sb.append("," + values._4(i - 1).replace(",", " "))
          } else {
            sb.append(",")
          }
        }
        val outRecord = outField + "," + values._3 + "," + values._1 + "," + match_percentage + "%," + values._2 + "," + unmatch_percentage + "%"

        //outRecord
        (outFieldPosition, (outRecord, sb.substring(1)))

      })

      //    outRDD.collect.foreach(println)
      val sortedRDD = outRDD.sortByKey(true)

      outRDD.cache()

      sortedRDD.cache()
      println("counts are " + sortedRDD.count() + "  out RDD " + outRDD.count() )

      val sortedArray = sortedRDD.collect
      d1d2.cache()
      d2d1.cache()


      println("**************************************************************************")
      sortedRDD.collect.foreach(println)
      sortedArray.foreach(f => {
        if (f._1 == -1) {
          fileStats = f._2
          println("fields key  " + f._2)
        } else {
          columnsReportList += f._2
          //        println("fields  " + f._2)
        }
      })

    }

    println("deepak is " + fileStats)
//    println("deepak is " + columnsReportList)


    /*
        val outputPath = args(6) + "/temp"
        val pathOutputPath = new Path(outputPath)
        val hadoopConf = sc.hadoopConfiguration
        val fs = pathOutputPath.getFileSystem(hadoopConf)
        fs.delete(pathOutputPath,true)

        outRDD.saveAsTextFile(outputPath)
    */

    val outFile = new File(args(6) + "/filesComparisonReport.csv")
    if (outFile.exists)
      outFile.delete()

    val onlyData1 = d1d2.count()
    val onlyData2 = d2d1.count()
    val fileLine1 = "File Level comparison report"
    val fileHeader1 = "File1,File2,File1⋃File2,File1-File2,File2-File1,File1⋂File2,Match Count,Match Percentage,UnMatch Count,Unmatch Percentage"
    val fileLevelValues = fileStats._1.split(",", -1)
    var fileOutStats = ""
    try {
      fileOutStats = data1Count + "," + data2Count + "," + union_count + "," + onlyData1 + "," + onlyData2 + "," + intersection_count + "," +
      fileLevelValues(2) + "," + fileLevelValues(3) + "," + fileLevelValues(4) + "," + fileLevelValues(5)
    } catch {
      case nfe: ArrayIndexOutOfBoundsException => throw new RuntimeException("array index exception for field " + fileStats._1)
    }

    val sampleDataHeader = "Key | File1 | File2,Key | File1 | File2,Key | File1 | File2,Key | File1 | File2,Key | File1 | File2"
    val sampleData = fileStats._2


    val columnsStatsHeader = "Field,Total Count,Matched Count,Matched Percentage,Unmatched Count,Unmatched Percentage," +
      "Key | File1 | File2,Key | File1 | File2,Key | File1 | File2,Key | File1 | File2,Key | File1 | File2"
    val bw = new BufferedWriter(new FileWriter(outFile))

    bw.write(fileLine1 + "\n")
    bw.write(fileHeader1 + "\n")
    bw.write(fileOutStats + "\n")
    bw.newLine
    bw.write(sampleDataHeader + "\n")
    bw.write(sampleData + "\n\n\n")
    bw.write(columnsStatsHeader+"\n")
    columnsReportList.foreach(f => {
      bw.write(f._1 + "," + f._2 + "\n")
    })
    if(onlyData1 > 0) {
      bw.write("\n" + "Keys from file1 only \n")
      d1d2.collect().foreach(f => {
        bw.write(f + "\n")
      })
    }
    if(onlyData2 > 0 ) {
      bw.write("\n" + "Keys from file2 only \n")
      d2d1.collect().foreach(f => {
        bw.write(f + "\n")
      })
    }
    bw.close


    //    val pathFile = new Path(outFile)
    //    fs.delete(pathFile,true)

    //    FileUtil.copyMerge(fs, pathOutputPath, fs, pathFile, true, hadoopConf, null)

  }


}
