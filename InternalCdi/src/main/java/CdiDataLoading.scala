  import java.io.File

  import org.apache.commons.io.FileUtils
  import org.apache.commons.lang3.StringUtils
  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.spark.HBaseContext
  import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
  import org.apache.hadoop.hbase.util.Bytes
  import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.{SparkConf, SparkContext}
  import net.liftweb.json.DefaultFormats
  import net.liftweb.json.JsonAST.JNothing

  import scala.io.Source

  object CdiDataLoading {


    def zip(x:String):String = {
      val streetRegex = "[^0-9]".r
      val ZipModified = streetRegex.replaceFirstIn(x,"~").trim
      val postModified = if(ZipModified.contains("~")) ZipModified.split("~")(0).trim else ZipModified.trim
      val PostalLength = postModified.length
      var zipReturn:String = ""

      if(PostalLength <= 5)
        zipReturn = StringUtils.leftPad(postModified,5,'0')
      else if (PostalLength > 5)
        zipReturn = postModified.substring(0,5)
      StringUtils.leftPad(zipReturn,5,'0')
    }

    val ZipCode = udf ((PreCdiZip:String) => {
      //val Postal: String = if (PreCdiZip != null || PreCdiZip.length > 0) PreCdiZip else ""
      //val streetRegex = "[^0-9]".r
      //val ZipModified = streetRegex.replaceFirstIn(PreCdiZip,"~").trim
      //val postModified = if(ZipModified.contains("~")) ZipModified.split("~")(0).trim else ZipModified.trim
      val PostalLength = PreCdiZip.length
      var zipReturn:String = ""

      if(PostalLength <= 5)
        zipReturn = StringUtils.leftPad(PreCdiZip,5,'0')
      else if (PostalLength > 5)
        zipReturn = PreCdiZip.substring(0,5)
      //StringUtils.leftPad(zipReturn,5,'0')

      zipReturn
    }
    )

    def main(args: Array[String]): Unit = {

      if (args.length != 8) {
        println("Required params")
        System.exit(1)
      }


      implicit val formats = DefaultFormats

      val PrecdiFileHeader = args(0)
      val PrecdiFileData = args(1)

      val PostCdiFileHeader = args(2)
      val PostCdiFileData = args(3)

      val Listcd: String = args(4)
      val PreCdiProps = args(5)
      //"/Users/Shared/Cloud Drive/My Cloud Documents/work/Loop/PrecdiJson.json"
      val PostCdiProps = args(6) //"/Users/Shared/Cloud Drive/My Cloud Documents/work/Loop/PostcdiJson.json"

      //val outlocation = args(7)

      val tableName = args(7) //"/mapr/rhea/axp/pcs/address_load_test"

      //if(new File(outlocation).exists())
      //FileUtils.deleteDirectory(new File(outlocation))

      val PrereadFile = Source.fromFile(PreCdiProps)
      val PostreadFile = Source.fromFile(PostCdiProps)

      val PreCdiparsedJson = net.liftweb.json.parse(PrereadFile.mkString)

      val PreAddrConfig = PreCdiparsedJson \ "pre_add"

      val listProp = PreAddrConfig \ s"$Listcd"

      if (!listProp.equals(JNothing)) {


        println(listProp)

      var sb = new StringBuilder()

      val sequenceNumber = (listProp \ "SEQUENCE_NUMBER").extract[String]
      val groupSequence = (listProp \ "GROUP_SEQUENCE").extract[String]
      val address = (listProp \ "address").extract[String]
      val city = (listProp \ "city").extract[String]
      val state = (listProp \ "state").extract[String]
      val zip = (listProp \ "zip").extract[String]


      val PostCdiparsedJson = net.liftweb.json.parse(PostreadFile.mkString)

      val PostAddrConfig = PostCdiparsedJson \ "post_add"

      //println(PostAddrConfig)

      val listPropPost = PostAddrConfig \ s"$Listcd"


        println(listPropPost)


        val PostsequenceNumber = (listPropPost \ "SEQUENCE_NUMBER").extract[String]
        val PostgroupSequence = (listPropPost \ "GROUP_SEQUENCE").extract[String]
        val cdi_addresstype = (listPropPost \ "cdi_addresstype").extract[String]
        val cdi_streetnumber = (listPropPost \ "cdi_streetnumber").extract[String]
        val cdi_predirectional = (listPropPost \ "cdi_predirectional").extract[String]
        val cdi_streetname = (listPropPost \ "cdi_streetname").extract[String]
        val cdi_thirdlineaddress = (listPropPost \ "cdi_thirdlineaddress").extract[String]
        val cdi_cityname = (listPropPost \ "cdi_cityname").extract[String]
        val cdi_state = (listPropPost \ "cdi_state").extract[String]
        val cdi_zipcode = (listPropPost \ "cdi_zipcode").extract[String]
        val cdi_ncoamoveeffectivedate = (listPropPost \ "cdi_ncoamoveeffectivedate").extract[String]


        sb.append(sequenceNumber).append(",").append(groupSequence).append(",").append(address).append(",").append(city).append(",").append(state).append(",").append(zip).append(",").
          append(cdi_addresstype).append(",").append(cdi_streetnumber).append(",").append(cdi_predirectional).append(",").append(cdi_streetname).append(",").
          append(cdi_thirdlineaddress).append(",").append(cdi_cityname).append(",").append(cdi_state).append(",").append(cdi_zipcode).append(",").append(cdi_ncoamoveeffectivedate)


        println("columns required" + sb.toString())

        val requiredColString = sb.toString().split(",").toList

        val sc = new SparkContext(new SparkConf())
        val sparksession = SparkSession.builder().enableHiveSupport().getOrCreate()

        val hbaseConfig = HBaseConfiguration.create()
        val hbaseContext = new HBaseContext(sc, hbaseConfig)


        import sparksession.implicits._

        val preDfSchema = sparksession.read.format("csv").option("header", true).option("sep", "\t").load(PrecdiFileHeader).schema
        val preDF = sparksession.read.format("csv").option("sep", "\t").schema(preDfSchema).load(PrecdiFileData)
          .select(s"$sequenceNumber", s"$groupSequence", s"$address", s"$city", s"$state", s"$zip")
          .withColumnRenamed(sequenceNumber, "presequenceNumber")
          .withColumnRenamed(groupSequence, "pregroupSequence")
          .withColumnRenamed(address, "preaddress")
          .withColumnRenamed(city, "precity")
          .withColumnRenamed(state, "prestate")
          .withColumnRenamed(zip, "prezip")


        val preZipDF = preDF.withColumn("zipMod", when('prezip.isNull, "").otherwise(ZipCode('prezip)))
          .withColumn("AddressConcatMd5", md5(lower(regexp_replace(concat_ws("", 'preaddress, 'precity, 'prestate, 'zipMod), "[^a-zA-Z0-9]+", ""))))
          .withColumn("PreCdiAddress", lower(regexp_replace(concat_ws("", 'preaddress, 'precity, 'prestate, 'zipMod), "[^a-zA-Z0-9]+", "")))

        val postDfSchema = sparksession.read.format("csv").option("header", true).option("sep", "\t").load(PostCdiFileHeader).schema
        val postDF = sparksession.read.format("csv").option("sep", "\t").schema(postDfSchema).load(PostCdiFileData)
          .select(s"$PostsequenceNumber", s"$PostgroupSequence", s"$cdi_addresstype", s"$cdi_streetnumber", s"$cdi_predirectional", s"$cdi_streetname", s"$cdi_thirdlineaddress", s"$cdi_cityname", s"$cdi_state", s"$cdi_zipcode", s"$cdi_ncoamoveeffectivedate")
          .withColumnRenamed(PostsequenceNumber, "postsequenceNumber")
          .withColumnRenamed(PostgroupSequence, "postgroupSequence")
          .withColumnRenamed(cdi_addresstype, "postcdi_addresstype")
          .withColumnRenamed(cdi_streetnumber, "postcdi_streetnumber")
          .withColumnRenamed(cdi_predirectional, "postcdi_predirectional")
          .withColumnRenamed(cdi_streetname, "postcdi_streetname")
          .withColumnRenamed(cdi_thirdlineaddress, "postcdi_thirdlineaddress")
          .withColumnRenamed(cdi_cityname, "postcdi_cityname")
          .withColumnRenamed(cdi_state, "postcdi_state")
          .withColumnRenamed(cdi_zipcode, "postcdi_zipcode")
          .withColumnRenamed(cdi_ncoamoveeffectivedate, "postcdi_ncoamoveeffectivedate")

        val notMovedAddress = postDF.where('postcdi_ncoamoveeffectivedate.isNull)
        val joinDF = preZipDF.join(notMovedAddress, 'presequenceNumber === 'postsequenceNumber && 'pregroupSequence === 'postgroupSequence)

        println(notMovedAddress.count())
        println(postDF.count())

        joinDF.printSchema()


        // val requiredCol = joinDF.select()

        //joinDF.printSchema()
        /*
      Zip Code
       */

        //val ZipModified = requiredCol.withColumn("zipMod",when(requiredCol(s"$zip").isNull,"").otherwise(lpad(substring(requiredCol(s"$zip"),0,5),5,"0")))


        // val ZipModified = requiredCol.withColumn("zipMod",when(requiredCol(s"$zip").isNull,"").otherwise(ZipCode(requiredCol(s"$zip"))))
        // ZipModified.printSchema()
        // val AddressConcat = ZipModified.withColumn("AddressConcatMd5",md5(lower(regexp_replace(concat_ws("",ZipModified(s"$address"),ZipModified(s"$city"),ZipModified(s"$state"),ZipModified("zipMod")),"[^a-zA-Z0-9]+","")))).withColumn("PreCdiAddress",lower(regexp_replace(concat_ws("",ZipModified(s"$address"),ZipModified(s"$city"),ZipModified(s"$state"),ZipModified("zipMod")),"[^a-zA-Z0-9]+","")))


        val prePost = joinDF.select('AddressConcatMd5, 'PreCdiAddress, 'postcdi_addresstype, 'postcdi_streetnumber, 'postcdi_predirectional, 'postcdi_streetname, 'postcdi_thirdlineaddress, 'postcdi_cityname, 'postcdi_state, 'postcdi_zipcode).where('PreCdiAddress.isNotNull)
        val columnsHashmap = prePost.columns
        //val columnsHashmap = prePost.columns.zipWithIndex
        //val columnsHashmap = prePost.drop("AddressConcatMd5").columns

        //prePost.write.format("csv").option("Header",false).option("sep","\t").save(outlocation+"/Data")
        //sc.parallelize(Seq(AddressConcat.columns.mkString("\t"))).coalesce(1).saveAsTextFile(outlocation+"/Header")

        println(columnsHashmap)
        println(columnsHashmap.length)


        /*
      Address Concat
       */

        val cf = Bytes.toBytes("cf1")
        prePost.rdd.hbaseForeachPartition(hbaseContext, (it, conn) => {
          println("inside connection block")
          val bufferedMutator = conn.getBufferedMutator(TableName.valueOf(tableName))
          it.foreach(f => {
            val put = new Put(Bytes.toBytes(f.getString(0)))
            put.addColumn(cf, Bytes.toBytes(columnsHashmap(1)), Bytes.toBytes(f.getString(1)))

            if (!f.isNullAt(2))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(2).substring(4)), Bytes.toBytes(f.getString(2)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(2).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(3))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(3).substring(4)), Bytes.toBytes(f.getString(3)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(3).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(4))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(4).substring(4)), Bytes.toBytes(f.getString(4)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(4).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(5))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(5).substring(4)), Bytes.toBytes(f.getString(5)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(5).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(6))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(6).substring(4)), Bytes.toBytes(f.getString(6)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(6).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(7))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(7).substring(4)), Bytes.toBytes(f.getString(7)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(7).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(8))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(8).substring(4)), Bytes.toBytes(f.getString(8)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(8).substring(4)), Bytes.toBytes(" "))

            if (!f.isNullAt(9))
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(9).substring(4)), Bytes.toBytes(f.getString(9)))
            else
              put.addColumn(cf, Bytes.toBytes(columnsHashmap(9).substring(4)), Bytes.toBytes(" "))


            /* for (x <- 1 until prePost.columns.length) {
            if(!f.isNullAt(x)) {
              val cn = Bytes.toBytes(columnsHashmap(x))
              val value = Bytes.toBytes(f.getString(x))
              put.addColumn(cf, cn, value)
            }
          }*/
            /*columnsHashmap.foreach(x => {
            if(f.getString(x._2).nonEmpty) {
              val cn = Bytes.toBytes(x._1)
              val value = Bytes.toBytes(f.getString(x._2))
              put.addColumn(cf, cn, value)
            }
          })*/

            bufferedMutator.mutate(put)
          })
          bufferedMutator.flush()
          bufferedMutator.close()
        })

      }
      else{
        println(s"Don't have properties in given config file $Listcd")
        System.exit(1)
      }
    }

  }
