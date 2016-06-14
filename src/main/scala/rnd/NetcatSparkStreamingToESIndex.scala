package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vgiridatabricks on 5/26/16.
  */
object NetcatSparkStreamingToESIndex {
  def main(args: Array[String]) : Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.toLowerCase.split(" "))
    words.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val wordsDataFrame = rdd.toDF("words")
      wordsDataFrame.registerTempTable("allwords")
      val wcdf = sqlContext.sql("select words,count(*) as total from allwords group by words")
      wcdf.show()
      import org.elasticsearch.spark.sql._
      wcdf.saveToEs("wordcount/wc")
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
