//import java.io.File
//import java.net.URL
//
//import org.apache.commons.io.FileUtils
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by gfp2ram on 11/1/2015.
// */
//object webCall {
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
//    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
//    val sc = new SparkContext(conf)
//    val hCtx = new org.apache.spark.sql.hive.HiveContext(sc)
////    val hCtx = new org.apache.spark.sql.SQLContext(sc)
//    val inputJSON = new File("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\babynames1.json")
//    FileUtils.copyURLToFile(new URL("https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json?accessType=DOWNLOAD"), inputJSON)
//    val toRDD = sc.textFile(inputJSON).toString()
//
//  }
//}
//
//
