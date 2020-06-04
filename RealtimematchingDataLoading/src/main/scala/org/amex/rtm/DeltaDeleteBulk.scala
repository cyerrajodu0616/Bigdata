package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DeltaDeleteBulk {

  def main(args: Array[String]): Unit = {


    val previousWarehouse = args(0)
    val currentWarehouse = args(1)

    val IndexName = args(2)
    val HostName = args(3)
    val PortN0 = args(4)

    val outputfolder = args(5)

    val Deletesql = "select b.recid from "+ currentWarehouse + " a right outer join " + previousWarehouse + " b on(a.recid = b.recid) where a.recid is null limit 10"

    val DeleteCurl = """curl -X POST """"+HostName+":"+PortN0+"/"+IndexName+"""/_delete_by_query" -H 'Content-Type: application/json' -d' { "query": { "match": { "recid":"""

    // Spark Configuration
    val sc = new SparkContext(new SparkConf().setAppName("Ajax_Tmid"))
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()
    import session.implicits._

    val DstMatch = session.sql(Deletesql).map(x => x.toString().replace("[","").replace("]","")).rdd.map(x => DeleteCurl+ """"""" + x + """""""+ """ } } } ' """ )


  }
}
