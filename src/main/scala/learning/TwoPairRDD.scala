package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 9/11/2015.
 */
object TwoPairRDD {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val collRDD1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val collRDD2 = sc.parallelize(List("ant", "falcon", "squid"), 2)
    val a = collRDD1.keyBy(_.length)
    val b = collRDD2.keyBy(_.length)
    //b.subtractByKey(a).collect
  }
}
