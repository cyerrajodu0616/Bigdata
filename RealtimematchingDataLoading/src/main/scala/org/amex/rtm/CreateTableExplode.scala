package org.amex.rtm

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

object CreateTableExplode {

  def main(args: Array[String]): Unit = {

    if( args.length != 5){
      println("required 5 arguments but received only " + args.length)
      System.exit(1)
    }


    val InputTable = args(0)
    val RecTypeFileMapping = args(1)
    val ouputTable = args(2)
    val script = args(3)
    val queueName= args(4)

    var LeadListcd = ""
    var AttrListcd = ""
    var SuppListcd = ""


    val file = new File(script)
    val bw = new BufferedWriter(new FileWriter(file))



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



    var explode = """set mapred.job.queue.name=""" + queueName + """; create table """ + ouputTable + """ as with exploded_Data as (select concat_ws("~",nvl(upper(business_name.busname_step1),''),
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
                                  from exploded_Data a join """ + InputTable + " b on (b.recid*1 = split(complete_Data,'~')[2]);"

    println(explode)

    bw.write(explode)
    bw.close()

  }

}
