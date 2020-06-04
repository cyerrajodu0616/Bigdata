package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object BulkIndexRequestCreation {

  def main(args: Array[String]): Unit = {

    val tableName = args(0)

    val sc = new SparkContext(new SparkConf())

    val hadoopconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopconf)
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()

    import session.implicits._

    val dstTable = session.read.table(tableName)



  }

}
