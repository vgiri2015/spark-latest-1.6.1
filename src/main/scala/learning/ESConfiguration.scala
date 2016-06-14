/*
 * Rule Based Engine Framework Development.
 */

package learning

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
// ES imports

object ESConfiguration {
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
  val sparkconf = new SparkConf().setAppName("ElasticSearch").setMaster("local").set("es.index.auto.create", "true")
    .set("es.nodes", "localhost")
  val sc = new SparkContext(sparkconf)

  def esConfig(hiveContext: HiveContext): DataFrame = {

    println("ES SPark Config: ")
    // Convert the MapWritable[Text, Text] to Map[String, String]
    val es_config = Map("path" -> "testcase_config/data", "es.nodes" -> "l01dml212.lab.csaa.pri", "pushdown" -> "true", "es.port" -> "9200", "es.http.timeout" -> "5m", "es.scroll.size" -> "50")
    // Spark 1.3 style
    val esSpark = hiveContext.load("org.elasticsearch.spark.sql", es_config)

    return esSpark
  }
}