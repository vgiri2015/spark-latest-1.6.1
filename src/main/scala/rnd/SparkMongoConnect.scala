package rnd

import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Varatharajan Giri Ramanathan on 5/5/16.
  */
object SparkMongoConnect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkMongoConnect")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val builder = MongodbConfigBuilder(Map(Host -> List("hostname:27017"), Database -> "test", Collection ->"testcollection", SamplingRatio -> 1.0, WriteConcern -> "normal"))
    val readConfig = builder.build()
    val mongoRDD = sqlContext.fromMongoDB(readConfig)
    mongoRDD.registerTempTable("testtable")
    val df = sqlContext.sql("SELECT * FROM testtable")
    //df.printSchema()
    df.take(30).foreach(println)
  }
}
