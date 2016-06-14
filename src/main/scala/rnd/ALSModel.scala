package rnd

/**
  * Created by vgiridatabricks on 6/13/16.
  */

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



object ALSModel {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
      .setMaster("local").setAppName("SparkMlALS")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Load and parse the data

    val data = sc.textFile("/Users/vgiridatabricks/Documents/aws_test")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
    val model = ALS.train(ratings,10,10,0.01)

  }
}

