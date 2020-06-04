package org.amex.rtm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object DeltaLoad {

  def main(args: Array[String]): Unit = {


    val previousWarehouse = args(0)
    val currentWarehouse = args(1)

    val dstIndexTable = args(2)

    val RecTypeFileMapping = args(3)

    val dst_match_rtm = args(4)



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


    val asupp = "when b.listcd*1 in ("+SuppListcd.drop(1)+") then 'SUPP' "
    val alead = "when b.listcd*1 in ("+LeadListcd.drop(1)+") then 'LEAD' "
    val aAttr = "when b.listcd*1 in ("+AttrListcd.drop(1)+") then 'ATTR' "



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
                            case """+ lead + Attr + supp + " else 'UNK' end ,nvl(upper(business_name.busname_step1),''), nvl(upper(matchtype),'')) as complete_Data from " + currentWarehouse + "." + dst_match_rtm

    explode = explode + """ LATERAL VIEW OUTER explode(business) itemTable AS business_name LATERAL VIEW OUTER explode(address) addTable as address_name
                            LATERAL VIEW OUTER explode(phone) PhoneTable as phone_no
                            LATERAL VIEW OUTER explode(businessid) businessTable as business_Id)
                            select complete_Data,0 as partid ,b.recid as recordid,2,case """ + alead + aAttr + asupp


    explode = explode + """  else 'UNK' end as rectype,
                                 b.vendorname as vendorname,
                                 b.listcd as listcode,
                                 b.matchtype as matchtype,
                                 b.business as business,
                                 b.businessid as businessid,
                                 b.person as person,
                                 b.personid as personid,
                                 b.address as address,
                                 b.phone as phone,
                                 b.email as email,
                                 b.siccode as siccode
                                  from exploded_Data a join """ + currentWarehouse + "."+ dst_match_rtm +" b on (b.recid*1 = split(complete_Data,'~')[2]) left join "+ previousWarehouse + "." + dst_match_rtm + " c on (b.recid*1 = c.recid*1) where c.recid is null "


    println("Sql Statement" + explode)
    // Spark Configuration
    val sc = new SparkContext(new SparkConf().setAppName("RTM_Delta_Load"))
    val session = SparkSession.builder().enableHiveSupport().getOrCreate()
    import session.implicits._

    val DstMatch = session.sql(explode).repartition(200)
    DstMatch.write.saveAsTable(dstIndexTable)

  }

}
