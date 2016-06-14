package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Varatharajan Giri Ramanathan on 2/4/2016.
 */
object CsvToESIngest
{
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("EsIndexDataLoad")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rfSplit = "rfTag,rfDescription,rfType,basicRFDaysRange,badsicRFSev,basicRFwgt,rollingYrsRFfreq,rollingYrsRFwght,rollingYrsRFmonthRange,rollingYrsRFmonthWght,rfKeyTag,additionalInfo"
    val rfSchema = schemaDef(rfSplit)
    val esConfig = Map(("es.nodes", "n01ssl103.aap.csaa.pri"), ("es.port", "9200"), ("es.index.auto.create", "true"), ("es.http.timeout", "5m"))
    val esdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "false").schema(rfSchema).load("C:\\Users\\gfp2ram\\Desktop\\horf_flag_rules.csv")
    //esdf.printSchema()
    esdf.registerTempTable("esdf")
    val esdfr = sqlContext.sql("select rfTag,rfType from esdf order by basicRFwgt desc")
    esdf.take(20).foreach(println)
    //val esdf = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema", "false").schema(rfSchema).load("/user/horf/horf_flag_rules.csv")
    //esdf.saveToEs("horfrulesindex/horf",cfg = esConfig)
  }

  def schemaDef(baseSchema: String): StructType = {
    return StructType(baseSchema.split(",").map(f => StructField(f, StringType, true)))
  }
}