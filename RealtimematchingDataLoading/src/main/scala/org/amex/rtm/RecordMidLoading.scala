package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object RecordMidLoading {

  def main(args: Array[String]): Unit = {


    if(args.length != 3){
      println("Required 3 parameters, but got: "+ args.length)
      System.exit(1)
    }
    val indexname = args(0)
    val inputfolder = args(1)
    val host = args(2)

    //case class PropertiesConfig(IndexName: String, Host: String, Port: String, IndexColumns: String)

    //case class IndexColumns(name: String, phone: String,city: String,state: String,zip: String,businessId: String,hashraw: String,recordid: String,busname: String,vendorname: String,listcode: String,addrtype: String,rectype: String,matchtype: String,completeRec: String)

    case class IndexColumns(recid: String, masterid: String)

    //case class IndexColumns2(name: String, businessId: String)

    //val prop = "/Users/Shared/Cloud Drive/My Cloud Documents/work/Realtime Matching/DataPreparation.properties"
    //val json = Source.fromFile(prop)
    //val parsedJson = net.liftweb.json.parse(json.mkString)
    //val ExtractingValues = parsedJson.extract[PropertiesConfig]



    val conf = new SparkConf().setAppName("Recordid_Mid_Indexing")

    // Setting up ElasticSearch configuration
    conf.set("spark.es.index.auto.create", "true")
    conf.set("spark.es.nodes","aeamxp04d8")
    conf.set("spark.es.port","9200")
    // conf.set("es.resource","realtimematching")

    val sc = new SparkContext(conf)

    val readData = sc.textFile(inputfolder).
      map(x => (x.split("\u0001")(0),x.split("\u0001")(1)))

    val EsData = readData.map(f => IndexColumns(f._1,f._2))

    EsSpark.saveToEs(EsData,indexname)


  }

}
