package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by gfp2ram on 1/10/2016.
  */
object WordCountDataSetAPI {
  def main(args:Array[String]) : Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //With Spark RDD Core-APIs
    //    val tf = sc.textFile("D:\\spark\\spark-1.5.0-bin-hadoop2.6\\README_test.md")
    //    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1)).filter(_ != "")
    //    val counts = splits.reduceByKey((x,y)=>x+y).take(10000).foreach(println)

    //With Spark DataSets API
    //Since the Dataset version of word count can take advantage of the built-in aggregate count,
    // this computation can not only be expressed with less code, but it will also execute significantly faster.
    import sqlContext.implicits._
    val tf = sqlContext.read.text("D:\\Spark\\spark-1.5.0-bin-hadoop2.6\\README_test.md").as[String]
    val splits = tf.flatMap(_.split(" ")).filter(_ !=" ")
    val counts = splits.groupBy(_.toLowerCase).count().take(10000).foreach(println)

    //splits.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\SplitOutput")
    //counts.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\CountOutput")
  }
}
