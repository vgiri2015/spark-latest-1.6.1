package rnd

import com.mongodb.hadoop.BSONFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

/**
  * Created by vgiridatabricks on 5/12/16.
  */

object BSONRead {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
      .setAppName("BSONFileReading")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val config = new Configuration()
    config.set("mongo.input.uri","mongodb://localhost:27017/test.nettuts")
    val mongoRDD = sc.newAPIHadoopFile("/usr/local/Cellar/mongodb-osx-x86_64-3.2.6/bin/dump/test/nettuts.bson",
      classOf[BSONFileInputFormat].asSubclass(classOf[org.apache.hadoop.mapreduce.lib.input.FileInputFormat[Object,
        BSONObject]]), classOf[Object], classOf[BSONObject]).take(30).foreach(println)
    mongoRDD
  }
}
