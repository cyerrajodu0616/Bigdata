
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import java.io.BufferedWriter
import org.apache.commons.io.FileUtils

object SequenceNumGenerator {

  def main(args: Array[String]): Unit = {
    case class hive_dwld(key: String, value: String)

    val in_table = args(0)
    val out_table = args(1)
    val start_rid = args(2)

    val tablelocation = new File(in_table).getAbsolutePath


    val spark = SparkSession
      .builder()
      .appName("Spark_Seq_Num_Generator")
      .config("spark.sql.table.loc", tablelocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val rddhive = sql("select * from "+in_table).toDF

    val hiveschema = rddhive.schema

    val inputrows = rddhive.rdd.zipWithIndex.map {
      case (r: Row, recid: Long) => { val id1 = start_rid.toLong+1 + recid; Row.fromSeq(id1 +: r.toSeq) }
    }
    val wcID = spark.createDataFrame(inputrows, StructType(StructField("recid", LongType, false) +: hiveschema.fields))

    wcID.createTempView("sprk_to_hive")
    sql("select max(recid) from sprk_to_hive")

    //sql("select max(recid) from sprk_to_hive").rdd.saveAsTextFile("/mapr/Helios/amexuserdata/AETPMC/jmasurka/matching/diy_count/diy_count_2.txt")
    sql("drop table if exists " +out_table)
    sql("create table "+out_table+" as select * from sprk_to_hive")

    println(wcID.count());

    val max_recid = sql("select max(recid) from "+ out_table ).show()
    //    println("Hive Max_Rec_id is" +max_recid.show())

    FileUtils.deleteQuietly(new File("/mapr/Helios/amexuserdata/AETPMC/jmasurka/matching/diy_count/diy_count.txt"))
    println ("File --> '/mapr/Helios/amexuserdata/AETPMC/jmasurka/matching/diy_count/diy_count.txt' is Deleted.")
    val file = new File("/mapr/Helios/amexuserdata/AETPMC/jmasurka/matching/diy_count/diy_count.txt")

    val bw = new BufferedWriter(new FileWriter(file))
    bw.write((wcID.count()+start_rid.toLong).toString())
    bw.close()


  }

}
