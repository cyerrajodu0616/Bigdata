package com.amex.utils

import java.io.{BufferedReader, File, FileReader}

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

object FileComparison {

  def main(args: Array[String]): Unit = {

    if (args.length != 7) {
      println("invalid usage of arguments")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("Length Check Utility").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext


    val dataFileName1: String = args(0)
    val dataFileName2: String = args(1)
    val headerFileName: String = args(3)

    val headerDelimiter: String = args(4)
    val dataDelimiter: String = args(2)
    val keyColumn: String = args(5).toLowerCase

    val br = new BufferedReader(new FileReader(new File(headerFileName)))
    val header = br.readLine.toLowerCase
    val headerLength = header.length
    val headerArray: Array[String] = header.split(headerDelimiter, -1)
    br.close

    val fieldPosition = new HashMap[String, Int]
    for (i <- 0 until headerArray.length)
      fieldPosition(headerArray(i)) = i

    val outColumnsSet = fieldPosition.keySet.-(keyColumn)

    import spark.implicits._
    val data1 = spark.createDataset(sc.textFile(dataFileName1))
    val data2 = spark.createDataset(sc.textFile(dataFileName2))


  }

}
