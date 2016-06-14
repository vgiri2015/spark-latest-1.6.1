package learning




/**
 * Created by gfp2ram on 9/18/2015.
 */
//object ESHive {
//  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
//    val sparkconf = new SparkConf().setAppName("ESHive").setMaster("local").set("es.index.auto.create", "true")
//    val sc = new SparkContext(sparkconf)
//    val hCtx = new SQLContext(sc)
//    //val hCtx1 = new HiveContext(sc)
//    import hCtx.implicits._
//    val inputFile = sc.textFile("C:\\RnD\\Workspace\\scala\\TestSpark\\testData\\peoplefile.txt").map(_.split(" ")).map(p => Person(p(0), p(1),p(2).trim.toInt)).toDF()
//    inputFile.registerTempTable("person")
//    val createtable = hCtx.sql("create table person_hive as select name,surname,age from person")
//    //val select = hCtx.sql("select name,surname,age from person")
//    //select.take(10).foreach(println)
//    //select.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
//  }
//  case class Person(name: String, surname: String, age: Int)
//}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gfp2ram on 1/30/2016.
 */
object ESHive {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val sparkconf = new SparkConf()
      .setAppName("ElasticSearch")
      .setMaster("local[*]")
    //      .set("es.nodes","localhost")
    //      .set("es.port","9200")
    val esConfig = Map("pushdown" -> "true", "es.nodes" -> "localhost", "es.port" -> "9200")
    val sc = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)
    val readESData = sqlContext.read.format("es").options(esConfig).load("hosiu_jan28/hosiu_index")
    readESData.printSchema()
  }
}
