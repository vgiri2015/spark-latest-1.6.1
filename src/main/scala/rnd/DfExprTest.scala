package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}

object DfExprTest {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(config = new SparkConf)
    val sqlContext = new SQLContext(sc)

    val spark_df = sqlContext.read.parquet("/mnt/card-voldemort/dln234/data/testfile.parquet", "50").cache()
    val pbadWindow = Window.partitionBy("ID").orderBy("MNTH_NUM")

    import org.apache.spark.sql.functions._
    val compRiskDf = spark_df.withColumn("OUT_ATTR1", expr(s"exp(${spark_df}.ATTR2_B)/(1+exp(${spark_df}.ATTR2_B)+exp(${spark_df}.ATTR1_B))"))
      .withColumn("OUT_ATTR2", expr("exp(ATTR1_B)/(1+exp(ATTR2_B)+exp(ATTR1_B))"))
      .withColumn("OUT_ATTR3", expr("exp(ATTR2_M)/(1+exp(ATTR2_M)+exp(ATTR1_M))"))
      .withColumn("OUT_ATTR4", expr("exp(ATTR1_M)/(1+exp(ATTR2_M)+exp(ATTR1_M))"))
      .withColumn("OUT_ATTR5", expr("exp(ATTR2)/(1+exp(ATTR2)+exp(ATTR1))"))
      .withColumn("OUT_ATTR6", expr("exp(ATTR1)/(1+exp(ATTR2)+exp(ATTR1))"))
      .withColumn("OUT_ATTR7", expr("exp(ATTR2)/(1+exp(ATTR2)+exp(ATTR1))"))
      .withColumn("OUT_ATTR8", expr("exp(ATTR1)/(1+exp(ATTR2)+exp(ATTR1))"))
      .withColumn("OUT_ATTR9_lagged", when(spark_df("MNTH_NUM") === 6, lag(spark_df("ATTR2_B").over(pbadWindow), 1))
        .when(spark_df("MNTH_NUM") === 7, lag(spark_df("ATTR2_B").over(pbadWindow), 2))
        .when(spark_df("MNTH_NUM") === 8, lag(spark_df("ATTR2_B").over(pbadWindow), 3))
        .otherwise(spark_df("ATTR2_B")))

    println(compRiskDf.count())
  }
}
