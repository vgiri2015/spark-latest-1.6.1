package rnd

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vgiridatabricks on 5/5/16.
  */
object CsvParsingEscape {

  def ThingIterator(row: Iterator[Row]): Unit = ???

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
      .setAppName("csvParser")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val addlOptions = Map(("header","false"),("delimiter","|"),("comment",null),("parseLib","univocity"),("mode","PERMISSIVE"),("quote",null))
    val esDF = sqlContext.read.format("com.databricks.spark.csv").options(addlOptions)
      //                .schema(maintenance)
      .load("/Users/vgiridatabricks/Downloads/csv_samples/escapecharacters.txt").registerTempTable("sparkcsvtable")
    //    //println(esDF.rdd.partitions.size)
  }
}