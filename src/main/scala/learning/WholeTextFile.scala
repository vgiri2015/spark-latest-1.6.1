package learning

/**
 * Created by gfp2ram on 9/6/2015.
 */

import org.apache.spark.{SparkConf, SparkContext}

object WholeTextFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("WholeTextFiles").setMaster("local")
    val sc = new SparkContext(conf)
    val inputRDD = sc.wholeTextFiles("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\wholetextfiles\\advprod\\")
    println(inputRDD.distinct())
    println(inputRDD.keys)
    println(inputRDD.values)
    val result = inputRDD.mapValues{y => val nums = y.split(" ").map(x => x.toDouble)
      nums.sum/nums.size.toDouble
    }
    //result.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\wholetextfiles\\advprod\\output.txt")
  }
}
