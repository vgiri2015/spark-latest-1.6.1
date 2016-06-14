package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vgiridatabricks on 5/21/16.
  */
object SparkCassandraRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      //      .set("spark.cassandra.auth.username", "username")
      //      .set("spark.cassandra.auth.password", "password")
      //      .set("spark.cassandra.connection.rpc.port","9160")
      .setMaster("local").setAppName("SparkCassandra")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val cc = new CassandraSQLContext(sc)

    //Just test Spark Cassandra Connectivity
    //    val tableConn = sc.cassandraTable("sparktestcassandra", "emp")
    //    print(tableConn)

    //Write to Cassandra Table
    //    val sampleRdd = sc.parallelize(Array(("Databricks", 101), ("Spark", 110), ("Big Data", 10),("Hadoop",40),("Falcon",50)))
    //    sampleRdd.saveToCassandra("sparktestcassandra", "wordcount", SomeColumns("word", "count"))

    //Read from Cassandra Table as a DataFrame
    val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "wordcount","keyspace" -> "sparktestcassandra"))
      .load()
    df.show()

    val df1 = cc.sql("select word,count from sparktestcassandra.wordcount")
    df1.toDF().show()
  }
}
