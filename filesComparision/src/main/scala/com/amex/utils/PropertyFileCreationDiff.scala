package main.scala.com.amex.utils

import java.io.{BufferedWriter, File, FileWriter}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

object PropertyFileCreationDiff {
  case class Column(name: String, bucket: List[String])
  case class TypeColDesc(`type`: String,cols: List[String])
  case class ImportanceColumn(name: String, value: String)
  case class Prop(email: String, queueName: String, datafile1: String,datafile2: String,headerFile1: String,headerFile2: String,dataDelimiter: String, headerDelimiter: String, keyColumn: String,outLocation: String,file1Label: String,file2Label: String,requiredColum: List[String],column: List[Column], importance: List[ImportanceColumn],columnType: List[TypeColDesc])
  //case class Prop(email: String, queueName: String, datafile1: String,datafile2: String,headerFile: String,dataDelimiter: String, headerDelimiter: String, keyColumn: String,outLocation: String,file1Label: String,file2Label: String,requiredColum: List[String],column: List[Column], importance: List[ImportanceColumn],columnType: List[TypeColDesc])
  def main(args: Array[String]): Unit = {

    implicit val formats = DefaultFormats

    if(args.length != 16  )
    {
      println(args.length)
      println("invalid usage of arguments")
      System.exit(1)
    }

    val Email: String = args(0)
    val Queue: String = args(1)
    val df1: String = args(2)
    val df2: String = args(3)
    val header:String = args(4)
    val dataDelim: String = args(5)
    val headerDelim:String = args(6)
    val keyColumn:String = args(7)
    val outLoc:String = args(8)
    val file1Label:String = args(11)
    val file2Label:String = args(12)
    val header2:String = args(15)


    var reqColumns: List[String] = List()


    val propOutFile = new File(outLoc + "/PropFile.Json")

    if (propOutFile.exists)
      propOutFile.delete()

    val bw = new BufferedWriter(new FileWriter(propOutFile))

    var ColNames: scala.collection.mutable.ListBuffer[Column] = scala.collection.mutable.ListBuffer()
    var ImporatanceColNames: scala.collection.mutable.ListBuffer[ImportanceColumn] = scala.collection.mutable.ListBuffer()
    val TypeCol: scala.collection.mutable.ListBuffer[TypeColDesc] = scala.collection.mutable.ListBuffer()



    if(!args(9).equalsIgnoreCase("ALL"))
    {
      reqColumns = args(9).split(",",-1).toList
    }

    if(!args(10).equalsIgnoreCase("none"))
    {
      val Columns = args(10).split(",",-1)
      Columns.foreach(x => {
        val cName = x.split(":")(0)
        val bucket = x.split(":",-1)(1).split("#",-1).toList
        ColNames.+=(Column(cName,bucket))
      }
      )
    }

    if(!args(13).equalsIgnoreCase("none"))
    {
      val Columns = args(13).split(",",-1)
      Columns.foreach(x => {
        val name = x.split(":")(0)
        val value = x.split(":",-1)(1)
        ImporatanceColNames.+=(ImportanceColumn(name,value))
      }
      )
    }

    if(!args(14).equalsIgnoreCase("none"))
    {
      val Columns = args(14).split(",",-1)
      Columns.foreach(x => {
        val `type` = x.split(":")(0)
        val cols = x.split(":",-1)(1).split("#",-1).toList
        TypeCol.+=(TypeColDesc(`type`,cols))
      }
      )
    }


    val propert = Prop(Email,Queue,df1,df2,header,header2,dataDelim,headerDelim,keyColumn,outLoc,file1Label,file2Label,reqColumns,ColNames.toList,ImporatanceColNames.toList,TypeCol.toList)
    //val propert = Prop(Email,Queue,df1,df2,header,dataDelim,headerDelim,keyColumn,outLoc,file1Label,file2Label,reqColumns,ColNames.toList,ImporatanceColNames.toList,TypeCol.toList)
    val jsonString = write(propert)
    bw.write(jsonString)
    bw.close()
    println(jsonString)



  }


}
