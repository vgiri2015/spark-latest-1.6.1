package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 9/17/2015.
 */
object aggregate {
  def main(args:Array[String]) {
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkconf = new SparkConf().setAppName("GroupByTesting").setMaster("local")
    val sc = new SparkContext(sparkconf)
    val inputRDD = sc.parallelize(List(1,2,3,3))
    val aggr = inputRDD.aggregate((0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2)
    )
    println(aggr)
    val avg = aggr._1/aggr._2.toFloat
    println(avg)
  }
}
