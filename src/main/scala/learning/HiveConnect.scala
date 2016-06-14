package learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by gfp2ram on 9/3/2015.
 */

object HiveConnect {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
//    val conf = new SparkConf().setAppName("HiveConnect").setMaster("yarn-client").set("spark.yarn.access.namenodes","hdfs://127.0.0.1:8020")
    val conf = new SparkConf().setAppName("HiveConnect").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.sql("create table yahoo_orc_table (date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume INT, adj_price FLOAT) stored as orc")
//    val yahoo_stocks = sc.textFile("hdfs://127.0.0.1:8020/tmp/yahoo_stocks.csv")
//    val header = yahoo_stocks.first
//    val data = yahoo_stocks.filter(_(0) != header(0))
//    case class YahooStockPrice(date: String, open: Float, high: Float, low: Float, close: Float, volume: Integer, adjClose: Float)
//    val stockprice = data.map(_.split(",")).map(row => YahooStockPrice(row(0), row(1).trim.toFloat, row(2).trim.toFloat, row(3).trim.toFloat, row(4).trim.toFloat, row(5).trim.toInt, row(6).trim.toFloat))
//    println(stockprice)
//    stockprice.registerTempTable("yahoo_stocks_temp")
//    val results = sqlContext.sql("SELECT * FROM yahoo_stocks_temp")
//    results.map(t => "Stock Entry: " + t.toString).collect().foreach(println)
//    results.saveAsOrcFile("yahoo_stocks_orc")
//    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
//    val yahoo_stocks_orc = sqlContext.orcFile("yahoo_stocks_orc")
//    yahoo_stocks_orc.registerTempTable("orcTest")
//    sqlContext.sql("SELECT * from orcTest").collect.foreach(println)
  }
}

