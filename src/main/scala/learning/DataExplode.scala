package learning

/**
 * Created by gfp2ram on 10/31/2015.
 */

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object DataExplode {

  case class schema1(key: String, values: Seq[String])

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("ExplodeApp").setMaster("local")
    val sc = new SparkContext(conf)
    val sCtx = new SQLContext(sc)
    import sCtx.implicits._
    val regpattern = """([A-Z][0-9]{1})|([A-Z][0-9]{2})""".r
    val txtfile = sc.parallelize(Seq("Row-Key-001, K1, 10, A2, 20, K3, 30, B4, 42, K5, 19, C20, 20",
      "Row-Key-002, X1, 20, Y6, 10, Z15, 35, X16, 42",
      "Row-Key-003, L4, 30, M10, 5, N12, 38, O14, 41, P13, 8"))
    val schema = "key,values"
    val pairs = txtfile.map(s => (s.split(",")(0), regpattern.findAllIn(s).toList))
    pairs.foreach(println)
    val pair_r = pairs.map({ case (x, (y)) => schema1(x, (y)) }).toDF()
    pair_r.foreach(println)
    pair_r.printSchema()
    val ex = pair_r.explode("values","word") {values : String => values.split(",")}
    val newex = ex.select("key","word")
    val newex_s = "key1,value1"
    val newex_row = newex.map({case (t) => Row.fromSeq(Seq(t.getString(0),t.getString(1)))})
    val newex_applySchema = StructType(newex_s.split(",").map(f => StructField(f, StringType, true)))
    val newexDF = sCtx.createDataFrame(newex_row,newex_applySchema)
    newexDF.printSchema()
  }
}
