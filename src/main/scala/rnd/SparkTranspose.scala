package rnd

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by vgiridatabricks on 5/28/16.
  */
object SparkTranspose {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
      .setAppName("csvParser")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val df = sc.parallelize(Seq(("Titanic","IronMan","JungleBook"),("1","10","20"),("2","30","40"),("1","30","20"))).toDF("a","b","c")
    df.registerTempTable("transpose")
    sqlContext.sql("select concat_ws(':',a.cola) as cola_new,concat_ws(':',a.colb) as colb_new,concat_ws(':',a.colc) as colc_new from (select collect_list(a) as cola, collect_list(b) as colb,collect_list(c) as colc from transpose)a").registerTempTable("tablesplitter")
    sqlContext.sql(
      """
  select split(cola_new,':') [0] as col1,split(cola_new,':') [1] as col2,split(cola_new,':') [2] as col3,split(cola_new,':') [3] as col4 from tablesplitter
  union all
  select split(colb_new,':') [0] as col1,split(colb_new,':') [1] as col2,split(colb_new,':') [2] as col3,split(colb_new,':') [3] as col4 from tablesplitter
  union all
  select split(colc_new,':') [0] as col1,split(colc_new,':') [1] as col2,split(colc_new,':') [2] as col3,split(colc_new,':') [3] as col4 from tablesplitter
      """)
      .show()

  }
}
