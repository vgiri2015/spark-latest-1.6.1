package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vgiridatabricks on 5/16/16.
  */
object ExplodeDf {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
      .setAppName("csvParser")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val df = sc.parallelize(Seq((1, Seq(2,3,4), Seq(5,6,7)), (2, Seq(3,4,5), Seq(6,7,8)), (3, Seq(4,5,6), Seq(7,8,9)))).toDF("a", "b", "c")
    val df1 = df.select(df("a"),explode(df("b")).alias("b_columns"),df("c"))
    val df2 = df1.select(df1("a"),df1("b_columns"),explode(df1("c").alias("c_columns"))).show()
  }

}
