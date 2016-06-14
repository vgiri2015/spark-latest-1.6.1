package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 9/11/2015.
 */
object OnePairRDD {
  def main(args:Array[String]):Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val collRDD = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    collRDD.reduceByKey((x,y) =>x+y).collect().foreach(println)
    collRDD.groupByKey().collect().foreach(println)
    collRDD.mapValues(x=>x+3).collect().foreach(println)
    collRDD.flatMapValues(x=>x to 10).collect.foreach(println)
    collRDD.keys.collect().foreach(println)
    collRDD.values.collect().foreach(println)
    collRDD.sortByKey().collect().foreach(println)

  }
}
