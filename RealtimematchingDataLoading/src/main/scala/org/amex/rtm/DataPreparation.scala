package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object DataPreparation {

  def main(args: Array[String]): Unit = {

    if( args.length != 3){
      println("required 3 arguments but received only " + args.length)
      System.exit(1)
    }

    val YearWeak = args(1)
    val RecTypeFileMapping = args(2)

    val indexTable=args(3)

    val dbname= "mwh_"+YearWeak
    val conf = new SparkConf().setAppName("RealtimeMAtchingIndexDataPreparation").setMaster("master")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()

    import session.implicits._

    val TableName = "mwh_"+YearWeak+".dst_match"
    val DstMatchTable = session.read.table(TableName)

    var LeadListcd = ""
    var AttrListcd = ""
    var SuppListcd = ""


    for (line <- Source.fromFile(RecTypeFileMapping).getLines()) {

      val listcd = line.split(",",-1)(0)
      val RecType = line.split(",",-1)(2)

      //hashMap += (listcd -> RecType)

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
                    |        nvl(upper(business_Id),''),
                    |        nvl(upper(recid),''),
                    |        nvl(upper(vendorname),''),
                    |        nvl(upper(address_name.state),''),
                    |        nvl(upper(address_name.zip),''),
                    |        nvl(upper(phone_no),''),
                    |        nvl(upper(hashraw),''),
                    |        nvl(upper(listcd),''),
                    |        nvl(upper(address_name.addrtype),''),
                    |        case """+ lead + Attr + supp + " else 'UNK' end,nvl(upper(business_name.busname_step1),''), nvl(upper(matchtype),'')) as complete_Data from " + dbname + ".dst_match"

    explode = explode + """ LATERAL VIEW OUTER explode(business) itemTable AS business_name LATERAL VIEW OUTER explode(address) addTable as address_name
                          |  LATERAL VIEW OUTER explode(phone) PhoneTable as phone_no
                          |  LATERAL VIEW OUTER explode(businessid) businessTable as business_Id)
                          |  select complete_Data,0 as partid ,recid as recordid,2,case """ + lead + Attr + supp


    explode = explode + """  else 'UNK' end as rectype,
                          |       vendorname as vendorname,
                          |       listcd as listcode,
                          |       matchtype as matchtype,
                          |       business as business,
                          |       businessid as businessid,
                          |       person as person,
                          |       personid as personid,
                          |       address as address,
                          |       phone as phone,
                          |       email as email,
                          |       siccode as siccode
                          |        from exploded_Data a join """ + dbname + ".dst_match b on (b.recid*1 = split(complete_Data,'~')[2])"


    val DstMatch = session.sql(explode)

    DstMatch.write.saveAsTable(indexTable)

  }

}
