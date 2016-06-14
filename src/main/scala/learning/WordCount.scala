package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 9/8/2015.
 */
object WordCount {
  def main(args:Array[String]) : Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val tf = sc.textFile("C:\\spark\\spark-1.4.1-bin-hadoop2.6\\README.md")
    //tf.map(l => l.length).reduce((a,b) =>a+b)
    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
    //We can use splits.reduceByKey(_+_) as well
    val counts = splits.reduceByKey((x,y)=>x+y)
    splits.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\SplitOutput")
    counts.saveAsTextFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\CountOutput")
  }
}
