package main.scala.com.amex.utils

import java.io.File


import net.liftweb.json.DefaultFormats

import scala.io.Source

object M7Loading {

  implicit val formats = DefaultFormats

  abstract class Json
  case class Stat_hard(Count: BigInt, MatchedCount: BigInt, MatchedPercentage: Float) extends Json
  case class Stat_10(Count_10Percent: BigInt, MatchedCount_10Percent: BigInt, MatchedPercentage_10Percent: Float) extends Json
  case class Stat_20(Count_20Percent: BigInt, MatchedCount_20Percent: BigInt, MatchedPercentage_20Percent: Float) extends Json
  case class Stat_30(Count_30Percent: BigInt, MatchedCount_30Percent: BigInt, MatchedPercentage_30Percent: Float) extends Json


  case class stat(Count: BigInt, MatchedCount: BigInt, MatchedPercentage: Float,Count_10Percent: BigInt, MatchedCount_10Percent: BigInt, MatchedPercentage_10Percent: Float,Count_20Percent: BigInt, MatchedCount_20Percent: BigInt, MatchedPercentage_20Percent: Float,Count_30Percent: BigInt, MatchedCount_30Percent: BigInt, MatchedPercentage_30Percent: Float)



  def main(args: Array[String]): Unit = {

    loadM7("/Users/Shared/Cloud Drive/My Cloud Documents/filesComp.csv",466);

  }


  def loadM7(csvFilePath: String, FeedId: Int): Unit  = {

    val fileReport = scala.io.Source.fromFile(csvFilePath).getLines()

      val x = fileReport.take(1).next()

      println(x)

      val file1 = x.split(",",-1)

      val file =  file1(3).split(" ",-1)(0)
      val file2 =  file1(4).split(" ",-1)(0)
      val notNull = "Not Null Count"
      val MatchedCount = "Matched Count"
      val MatchPercentage = "Matched Percentage"

      println(file1.size)

      val numberOfStats = (file1.size - 5)/5

      println(numberOfStats)

      fileReport.take(1).foreach(println(_))




  }


}
