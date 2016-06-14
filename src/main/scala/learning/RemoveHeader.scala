package learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gfp2ram on 10/30/2015.
 */
object RemoveHeader {
  case class csvData(sno:Int,alpha:String,nickname:String)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val sparkconf = new SparkConf().setAppName("ESHive").setMaster("local").set("es.index.auto.create", "true")
    val sc = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val full_csv = sc.parallelize(Array(
      "column1, column2, column3",
      "122290, Giri, Saama",
      "122291, Mathan, Saama1",
      "122292, Venu, Saama2",
      "122293, Rajeev, Cogni",
      "122294, Parthi, Cogni",
      "122295, Parthib, Saama",
      "122296, Gaurav, Infosys",
      "122297, Giri, Saama"
      ))
    val split_csv  = full_csv.map(_.split(","))
    val header = split_csv.first
    //Remove Header
    val csv_data = split_csv.filter(_(0) != header(0))
    val csvDF = csv_data.map(r => csvData(r(0).toInt,r(1),r(2))).toDF()
    csvDF.registerTempTable("csv_samples")
    val query_output = sqlContext.sql("select sno,alpha,nickname from csv_samples").take(10).foreach(println)
  }
}
