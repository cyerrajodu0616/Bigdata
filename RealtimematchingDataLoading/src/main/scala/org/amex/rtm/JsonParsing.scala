package org.amex.rtm

import scala.io._
import net.liftweb.json._

object JsonParsing {

  def main(args: Array[String]): Unit = {

    implicit val formats = DefaultFormats
    case class PropertiesConfig(IndexName: String, Host: String, Port: String, IndexColumns: String)


    val prop = "/Users/Shared/Cloud Drive/My Cloud Documents/work/Realtime Matching/DataPreparation.properties"
    val json = Source.fromFile(prop)
    val parsedJson = net.liftweb.json.parse(json.mkString)
    println(parsedJson)

    val m = parsedJson.extract[PropertiesConfig]

    println(m.Host)
    println(m.IndexColumns)
    print(m.IndexName)
    println(m.Port)

  }

}
