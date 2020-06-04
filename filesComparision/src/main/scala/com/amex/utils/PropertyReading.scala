package main.scala.com.amex.utils

import scala.io.Source
import net.liftweb.json.DefaultFormats

object PropertyReading {

  def main(args: Array[String]): Unit = {

    implicit val formats = DefaultFormats


    case class Column(column: List[Prop])
    case class Prop(name:String, bucket:List[String])


    val filename = "/Users/Shared/prop.json"

    val ConfigFileName = filename.split("/").last

    println(ConfigFileName)

    val prop = Source.fromFile(filename)

    val parsedJson = net.liftweb.json.parse(prop.mkString)
    println(parsedJson)

    val OutputTable = (parsedJson \ "column").children

    println(OutputTable)
    var SchemaDT: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap()
    println( (parsedJson\ "datafile1").extract[String])

    val RequiredColumns = (parsedJson \ "requiredColum").children

    if(RequiredColumns.size != 0 )
      {
        for( z <- RequiredColumns)
          println(z.extract[String])
      }



    for( x <- OutputTable){
      println((x \ "name").extract[String])
      val Percent = (x \ "bucket").children
      println(Percent)

      val bucketpercent: StringBuilder = new StringBuilder()
      for(y <-  Percent){
        println(y.extract[String])
        bucketpercent.append(y.extract[String]).append(",")
      }
      println(bucketpercent.dropRight(1).toString())

      SchemaDT.+=((x \ "name").extract[String] -> bucketpercent.dropRight(1).toString())
      println(x)
    }



    println(SchemaDT.get("sit_mzp_id").getOrElse(""))





  }


}
