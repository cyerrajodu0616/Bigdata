package main.scala


import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes


object load {

  implicit val formats = DefaultFormats

  //case class stat(Count: String, MatchedCount: String, MatchedPercentage: String,Count_10Percent: String, MatchedCount_10Percent: String, MatchedPercentage_10Percent: String,Count_20Percent: String, MatchedCount_20Percent: String, MatchedPercentage_20Percent: String,Count_30Percent: String, MatchedCount_30Percent: String, MatchedPercentage_30Percent: String)

  case class values(Type: String, Count: String, MatchedCount: String, MatchedPercentage: String)
  //case class vale(keys : scala.collection.mutable.LinkedHashMap[String,values])
  //case class stats(column: String,feedId: String,date1: String, date2: String,rundate: String,statistics : scala.collection.mutable.LinkedHashMap[String,values])
 // case class buck(bucktype: String,stat:scala.collection.mutable.[String,values])


  case class stats(column: String,feedId: String,date1: String, date2: String,rundate: String,statistics : scala.collection.mutable.ListBuffer[values])

  case class FeedStats(feedId:String, date1: String, date2: String, rundate: String,file1Count: String, file2Count: String, MatchCount: String, PercentageDifference: String)

  //case class va(x : Map[String,values])

  def getHbaseTable(tableName: String) = {
    val config = HBaseConfiguration.create
    config.set("mapr.hbase.default.db", "maprdb")
    val conn = ConnectionFactory.createConnection(config)
    conn.getTable(TableName.valueOf(tableName))
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 7){
      println("required 7 parameters but received " + args.length)
      System.exit(1)
    }

    val fuzzyOut:String =  args(0)
    val feedId:String = args(1)
    val date1:String = args(2)
    val date2:String = args(3)
    val rundate:String = args(4)
    val tableName: String = args(5)
    val keycolumn: String = args(6)

   /* val fuzzyOut = "/Users/cyerradj/Documents/filesComparisonReportNew.csv"  //args(0)
    val feedId:String = "1234" //args(1)
    val date1:String = "123456" //args(2)
    val date2:String = "12345" // args(3)
    val rundate:String = "12e32" // args(4)
    val tableName: String = "/mapr/rhea/axp/qc/qc_table_test" //args(5)
    val keycolumn: String = "sit_mzp_id" // args(6)*/

    val feedOut:String = fuzzyOut.split("/").dropRight(1).mkString("/").concat("/filesComparison.csv")


    val fileReport = scala.io.Source.fromFile(fuzzyOut).getLines()
    val feedReport = scala.io.Source.fromFile(feedOut).getLines()

    val feedStat = feedReport.take(1).next().split(",",-1)
    val header = fileReport.take(1).next()

    println(header)

    val line1 = header.split(",", -1)

    val file1 = line1(3).split(" ", -1)(0)
    val file2 = line1(4).split(" ", -1)(0)
    val notNull = "Not Null Count"
    val MatchedCount = "Matched Count"
    val MatchPercentage = "Matched Percentage"

    val buckets = (line1.size/5)-1

    println(line1.size)

    var Index: Int = 5

    val table = getHbaseTable(tableName)



      val file1Cnt = BigInt(feedStat(0))
      val file2Cnt = BigInt(feedStat(1))
      val matchedcnt = BigInt(feedStat(2))
      val matchedPercentage = BigDecimal(feedStat(3))
      val matchDifference:Float = 100 - matchedPercentage.toFloat
      val dataDifference = BigDecimal(((file1Cnt-file2Cnt).toFloat*100.0f/file2Cnt.toFloat).abs).setScale(4,BigDecimal.RoundingMode.HALF_UP).toDouble



    //71536448,68334723,68253127,100
      val fuzzyStat = FeedStats(feedId,date1,date2,rundate,file1Cnt.toString(),file2Cnt.toString(),matchedcnt.toString(),dataDifference.toString)
      val FeedjsonString = write(fuzzyStat)
      println(FeedjsonString)

      val Feedkey = feedId + "_" + date1 + "_" + date2
      println(Feedkey)

      val Feedput = new Put(Bytes.toBytes(Feedkey))
      val cf = "fuzzy"
      Feedput.addColumn(Bytes.toBytes(cf),Bytes.toBytes("stats"),Bytes.toBytes(FeedjsonString))
      Feedput.addColumn(Bytes.toBytes(cf),Bytes.toBytes("FeedId"),Bytes.toBytes(feedId))
      Feedput.addColumn(Bytes.toBytes(cf),Bytes.toBytes("date1"),Bytes.toBytes(date1))
      Feedput.addColumn(Bytes.toBytes(cf),Bytes.toBytes("date2"),Bytes.toBytes(date2))
      Feedput.addColumn(Bytes.toBytes(cf),Bytes.toBytes("rundate"),Bytes.toBytes(rundate))
      Feedput.addColumn(Bytes.toBytes(cf),Bytes.toBytes("qcType"),Bytes.toBytes("FeedLevel"))

    table.put(Feedput)


    if (fileReport.nonEmpty) fileReport.foreach(x => {
      val record = x.split(",", -1)
      val column: String = record(0)
      val key = feedId + "_" + column + "_" + date1 + "_" + date2

     // var CoulumnPercent: scala.collection.mutable.LinkedHashMap[String, values] = scala.collection.mutable.LinkedHashMap()
      var CoulumnPercentset: scala.collection.mutable.ListBuffer[values] = scala.collection.mutable.ListBuffer()
     // var CoulumnPercentsetFeed: scala.collection.mutable.ListBuffer[values] = scala.collection.mutable.ListBuffer()


      var y = 1

      while(y < buckets ) {
          val key = line1((Index * y) + 1)
          //CoulumnPercent.+=(key -> values(key, record(Index * y), record((Index * y) + 1), record((Index * y) + 2)))


          val perc = if(record((Index * y) + 2).contentEquals("N/A")) "N/A" else record((Index * y) + 2).replace("%","").toFloat.toString
          CoulumnPercentset.+=(values(key, record(Index * y), record((Index * y) + 1), perc))


            //BigDecimal(record((Index * y) + 2)).setScale(4,BigDecimal.RoundingMode.HALF_UP).toString)
        y += 1
      }

      val fuzzyStat = stats(column,feedId,date1,date2,rundate,CoulumnPercentset)

      implicit val formats = DefaultFormats
      val jsonString = write(fuzzyStat)
      println(jsonString)




      println(key)
      val put = new Put(Bytes.toBytes(key))
      val cf = "fuzzy"
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("stats"),Bytes.toBytes(jsonString))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("FeedId"),Bytes.toBytes(feedId))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("Column"),Bytes.toBytes(column))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("date1"),Bytes.toBytes(date1))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("date2"),Bytes.toBytes(date2))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("rundate"),Bytes.toBytes(rundate))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("qcType"),Bytes.toBytes("AttribLevel"))
      table.put(put)



      //sit_mzp_id,N/A,N/A,68253127,68253127,68253127,68253127,100%, , ,N/A,N/A,N/A, , ,N/A,N/A,N/A, , ,N/A,N/A,N/A, , ,N/A,N/A,N/A, , ,N/A,N/A,N/A, ,

    })


  }

}
