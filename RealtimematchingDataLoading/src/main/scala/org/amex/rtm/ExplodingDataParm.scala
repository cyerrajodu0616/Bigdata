package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object ExplodingDataParm {

  def main(args: Array[String]): Unit = {


    if( args.length != 3){
      println("required 3 arguments but received only " + args.length)
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("RealtimeMAtchingIndexDataPreparation")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()

    val InputTable = args(0)
    val RecTypeFileMapping = args(1)
    val ouputTable = args(2)

    var LeadListcd = ""
    var AttrListcd = ""
    var SuppListcd = ""


    for (line <- Source.fromFile(RecTypeFileMapping).getLines()) {

      val listcd = line.split(",",-1)(0)
      val RecType = line.split(",",-1)(2)

      if( RecType.equalsIgnoreCase("SUPP"))
      {
        SuppListcd = SuppListcd + "," + listcd
      }
      else if ( RecType.equalsIgnoreCase("LEAD")){

        LeadListcd = LeadListcd + "," + listcd

      }
      else if ( RecType.equalsIgnoreCase("ATTR")){
        AttrListcd = AttrListcd + "," + listcd
      }

    }

    val supp = "when listcd*1 in ("+SuppListcd.drop(1)+") then 'SUPP' "
    val lead = "when listcd*1 in ("+LeadListcd.drop(1)+") then 'LEAD' "
    val Attr = "when listcd*1 in ("+AttrListcd.drop(1)+") then 'ATTR' "



    var explode = """ with exploded_Data as (select concat_ws("~",nvl(upper(business_name.busname_step1),''),
                            nvl(upper(business_Id),''),
                            nvl(upper(recid),''),
                            nvl(upper(vendorname),''),
                            nvl(upper(address_name.state),''),
                            nvl(upper(address_name.zip),''),
                            nvl(upper(phone_no),''),
                            nvl(upper(hashraw),''),
                            nvl(upper(listcd),''),
                            nvl(upper(address_name.addrtype),''),
                            case """+ lead + Attr + supp + " else 'UNK' end,nvl(upper(business_name.busname_step1),''), nvl(upper(matchtype),''), nvl(upper(mid),'')) as complete_Data from " + InputTable

    explode = explode + """ LATERAL VIEW OUTER explode(business) itemTable AS business_name LATERAL VIEW OUTER explode(address) addTable as address_name
                            LATERAL VIEW OUTER explode(phone) PhoneTable as phone_no
                            LATERAL VIEW OUTER explode(businessid) businessTable as business_Id)
                            select complete_Data,0 as partid ,recid as recordid,2,case """ + lead + Attr + supp


    explode = explode + """  else 'UNK' end as rectype,
                                 vendorname as vendorname,
                                 listcd as listcode,
                                 matchtype as matchtype,
                                 business as business,
                                 businessid as businessid,
                                 person as person,
                                 personid as personid,
                                 address as address,
                                 phone as phone,
                                 email as email,
                                 siccode as siccode
                                  from exploded_Data a join """ + InputTable + " b on (b.recid*1 = split(complete_Data,'~')[2])"


    println(explode)



    val DstMatch = session.sql(explode)

    var Options = scala.collection.mutable.Map[String, String]()

    Options += ("fileFormat" -> "textfile")
    Options += ("inputFormat" -> "org.apache.hadoop.mapred.TextInputFormat")
    Options += ("outputFormat" -> "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
    Options += ("serde" -> "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")



    //println(DstMatch)

    //DstMatch.write.text("/mapr/Helios/amexprod/RealTimeMatching/dstmatch/test")

    DstMatch.write.options(Options).saveAsTable(ouputTable)

    //DstMatch.write.save("/mapr/Helios/amexprod/RealTimeMatching/dstmatch/test")

    //DstMatch.write.saveAsTable(ouputTable)





  }

}
