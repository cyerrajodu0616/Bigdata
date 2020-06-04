package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DeleteONRecordid {

  def main(args: Array[String]): Unit = {

    val DeleteRecid = args(0)
    val columnname = args(1)

    val IndexName = args(2)
    val HostName = args(3)
    val PortN0 = args(4)

    val outputfolder = args(5)

    val Deletesql = "select "+ columnname +" from "+ DeleteRecid

    val DeleteCurl = """curl -X POST """"+HostName+":"+PortN0+"/"+IndexName+"""/_delete_by_query" -H 'Content-Type: application/json' -d' { "query": { "match": { "recid":"""

    // Spark Configuration
    val sc = new SparkContext(new SparkConf().setAppName("Delete Recordid RTM"))
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()
    import session.implicits._

    val DstMatch = session.sql(Deletesql).map(x => x.toString().replace("[","").replace("]","")).rdd.map(x => DeleteCurl+ """"""" + x + """""""+ """ } } } ' """ )

    DstMatch.saveAsTextFile(outputfolder)

  }

}
