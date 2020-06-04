package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object WriteESWithID {

  def main(args: Array[String]): Unit = {


    if(args.length != 2){
      println("Required 2 parameters, but got: "+ args.length)
      System.exit(1)
    }
    val indexname = args(0)
    val inputfolder = args(1)

    //case class PropertiesConfig(IndexName: String, Host: String, Port: String, IndexColumns: String)

    //case class IndexColumns(name: String, phone: String,city: String,state: String,zip: String,businessId: String,hashraw: String,recordid: String,busname: String,vendorname: String,listcode: String,addrtype: String,rectype: String,matchtype: String,completeRec: String)

    case class IndexColumns1(name: String, businessId: String,recid: String,vendorname: String,state: String,zip: String,phone: String,hashraw: String,listcode: String,addrtype: String,rectype: String,busname: String,matchtype: String,completeRec: String)

    //case class IndexColumns2(name: String, businessId: String)

    //val prop = "/Users/Shared/Cloud Drive/My Cloud Documents/work/Realtime Matching/DataPreparation.properties"
    //val json = Source.fromFile(prop)
    //val parsedJson = net.liftweb.json.parse(json.mkString)
    //val ExtractingValues = parsedJson.extract[PropertiesConfig]



    val conf = new SparkConf().setAppName("Dst_Match_Indexing")

    // Setting up ElasticSearch configuration
    conf.set("spark.es.index.auto.create", "true")
    conf.set("spark.es.nodes","aeamxp04d8")
    conf.set("spark.es.port","9200")
    // conf.set("es.resource","realtimematching")
    conf.set("spark.es.mapping.routing","state")

    val sc = new SparkContext(conf)

    val readData = sc.textFile(inputfolder).map(x => (x.split("\u0001")(0).split("~"),x.split("\u0001").drop(1).mkString("\u0001")))

    val EsData = readData.map(f => IndexColumns1(f._1(0),f._1(1),f._1(2),f._1(3),f._1(4),f._1(5),f._1(6),f._1(7),f._1(8),f._1(9),f._1(10),f._1(11),f._1(12),f._2))

    EsSpark.saveToEs(EsData,indexname)



  }


}
