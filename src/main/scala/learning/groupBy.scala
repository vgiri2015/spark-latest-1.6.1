package learning

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gfp2ram on 9/15/2015.
 */
object groupBy {
  def main(args:Array[String]) {
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkconf = new SparkConf().setAppName("GroupByTesting").setMaster("local")
    val sc = new SparkContext(sparkconf)
    case class Item(id:String,name:String,unit:Int,companyID:String)
    case class Company(companyid:String,name:String,city:String)
    val i1 = Item("1","first",2,"c1")
    val i2 = i1.copy(id="2",name="second")
    val i3 = i1.copy(id="3",name="third",companyID ="c2")
    val items =  sc.parallelize(List(i1,i2,i3))
    //items.collect().foreach(println)
    val c1 = Company("c1","company-1","city-1")
    val c2 = Company("c2","company-2","city-2")
    val companies = sc.parallelize(List(c1,c2))
    //companies.collect().foreach(println)
    val groupedItems = items.groupBy(x =>x.companyID)
    //groupedItems.collect().foreach(println)
    val groupedbycompanies = companies.groupBy(x=>x.companyid)
    //groupedbycompanies.collect().foreach(println)
    groupedItems.join(groupedbycompanies).take(10).foreach(println)
  }
}
