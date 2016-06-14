package learning

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gfp2ram on 10/28/2015.
 */
object testHDFS {
  def main(args: Array[String]): Unit = {
    //All the Below Properties will be set through spark-submit --conf options during Packaging and Deployment.
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("testHDFS").setMaster("yarn-client")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.spill.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.executor.memory", "100g")
      .set("spark.driver.memory", "40g")
    val sc = new SparkContext(conf)
    val sbfile = sc.textFile("hdfs://p01bdl823.aap.csaa.com:8020/user/xgfp2ram/myPigJobs/Batting.csv")
    sbfile.first()
  }
}