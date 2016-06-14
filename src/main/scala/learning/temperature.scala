package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 10/20/2015.
 */
object temperature {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val sparkconf = new SparkConf().setAppName("Temperatur").setMaster("local")
    val sc = new SparkContext(sparkconf)
    val input = sc.textFile("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\temperature")
    val mapinput = input.map(x => x.split(",")).foreach(println)
  }
}
