package rnd

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gfp2ram on 1/4/2016.
  */
object ClusterBy {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("ClusterBy").setMaster("local[2]").set("hive.exec.scratchdir","D:\\hadoop\\")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //Creation of Table 2
    val array1 = for (i <- List.range(1, 10000000)) yield Row(i, i % 2)
    val schema1 =
      StructType(
        StructField("num1", IntegerType, true) ::
          StructField("bit1", IntegerType, true) :: Nil)

    val dataFrame1 = sqlContext.createDataFrame(sc.parallelize(array1, 200), schema1)
    //    dataFrame1.write.parquet("D:\\Spark\\array1.parquet")
    dataFrame1.registerTempTable("large_table_1")

    //Creation of Table 2
    val array2 = for (i <- List.range(1, 10000000)) yield Row(i, i % 3)

    val schema2 =
      StructType(
        StructField("num2", IntegerType, true) ::
          StructField("bit2", IntegerType, true) :: Nil)

    val dataFrame2 = sqlContext.createDataFrame(sc.parallelize(array2, 200), schema2)
    //    dataFrame2.write.parquet("D:\\Spark\\array2.parquet")
    dataFrame2.registerTempTable("large_table_2")

    sqlContext.cacheTable("large_table_1")
    sqlContext.cacheTable("large_table_2")

    val a = sqlContext.sql("select * from large_table_1 cluster by num1").registerTempTable("sorted_large_table_1")
    val b = sqlContext.sql("select * from large_table_2 cluster by num2").registerTempTable("sorted_large_table_2")
    val c = sqlContext.sql("create table sorted_large_joined_table as select * from sorted_large_table_1 " +
      "join sorted_large_table_2 on sorted_large_table_1.num1 = sorted_large_table_2.num2")
    c.printSchema()
    //    val parq1 = sqlContext.read.parquet("D:\\Spark\\array1.parquet")
    //    val parq2 = sqlContext.read.parquet("D:\\Spark\\array2.parquet")
  }
}
