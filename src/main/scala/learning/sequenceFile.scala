package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 9/15/2015.
 */
object sequenceFile {
  def main(args:Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val sparkconf = new SparkConf().setAppName("SequenceFile").setMaster("local")
    val sc = new SparkContext(sparkconf)
    val data = sc.parallelize(List(("Panda",3),("Kay",6),("Snail",2)))
    data.saveAsSequenceFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\seqFileOutput")
  }
}
