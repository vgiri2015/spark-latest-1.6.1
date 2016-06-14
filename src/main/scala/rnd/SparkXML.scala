package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gfp2ram on 1/7/2016.
 */
object SparkXML {
  case class pomData (artifactId:String,dependencies:Seq[String],groupId:String,modelVersion:String,name:String,packaging:String)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("SparkXMLParsing").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "ClaimInvestigationAddRs")
//      .option("rowTag", "project")
//      //.load("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\xmlinput.xml")
      .load("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\iso.1440022328623.xml").printSchema()
//      //.load("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\views.xml")
//      .load("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\pom.xml")
//    //df.printSchema()
//    //val selectedData = df.select("author","id","title","genre","price","publish_date","description")
////    val selectedData = df.select("ClaimsOccurrence.ItemIdInfo.InsurerId",
////      "Policy.PolicyNumber","AdjusterParty.GeneralPartyInfo.NameInfo.PersonName",
////      "ClaimsOccurrence.ItemIdInfo.AgencyId",
////    "ClaimsOccurrence.LossDt",
////    "AdjusterParty.AdjusterPartyInfo.LossCauseCd",
////    "AdjusterParty.AdjusterPartyInfo.CoverageCd").take(5).foreach(println)
//    //val selectedData = df.select("view.name").distinct().take(3).foreach(println)
//    //selectedData.printSchema()
//    //selectedData.take(2).foreach(println)
//    //val res = sqlContext.sql("select * from consumer")
//    //res.take(20).foreach(println)
//    //    selectedData.write
////      .format("com.databricks.spark.xml")
////      .option("rootTag", "books")
////      .option("rowTag", "book")
////      .save("newbooks.xml")
//    val pomData = df.select("artifactId","dependencies","groupId","modelVersion","name","packaging")
//    pomData.registerTempTable("pomData")
//    sqlContext.sql("select artifactId,groupId,modelVersion,name,packaging,dependencies from pomData").distinct().take(3).foreach(println)
//    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "project").load("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\pom.xml")
//    df.registerTempTable("xmldf")
//    val xmldfhive = sqlContext.sql("select artifactId,build,dependencies,modelversion,name,packaging," +
      //"url,version,groupid,parent from xmldf")
    //xmldfhive
    /*
    xmldfhive: org.apache.spark.sql.DataFrame =
    [artifactId: string,
    build: struct<finalName:string>,
    dependencies: struct<dependency:array<struct<artifactId:string,groupId:string,scope:string,version:string>>>,
    groupId: string,
    modelVersion: string,
    name: string,
    packaging: string,
    parent: struct<artifactId:string,groupId:string,version:string>,
    url: string,
    version: string,
    xmlns: string,
    xmlns:xsi: string,
    xsi:schemaLocation: string]
    */
//    sqlContext.sql("drop table if exists default.POMXML")
//    sqlContext.sql("CREATE TABLE default.POMXML(artifactid string,build struct<finalname:string>,dependencies struct<dependency:array<struct<artifactId:string,groupId:string,scope:string,version:string>>>,modelVersion string,name string,packaging string,url string,version string,groupId string, parent struct<artifactId:string,groupId:string,version:string>) STORED AS PARQUET")
//    sqlContext.sql("dfs -rmr /user/horf/pomxmldata")
//    xmldfhive.write.parquet("/user/horf/pomxmldata")
//    sqlContext.sql("load data inpath '/user/horf/pomxmldata' into table IMDP.POMXML")
//    val pomxmlhive = sqlContext.sql("select artifactid,build.finalname as finalname," +
//      "dependencies.dependency.artifactid as departifactid,dependencies.dependency.groupid as depgroupid," +
//      "dependencies.dependency.scope as depscope,dependencies.dependency.version as depversion,modelversion,name as projectname," +
//      "packaging,url,version,groupid as groupid,parent.artifactId as parentartifactid,parent.groupId as parentgroupid," +
//      "parent.version as parentversion from default.pomxml")
//    pomxmlhive.printSchema()
//
//    /*
//    root
//     |-- artifactId: string (nullable = true)
//     |-- finalname: string (nullable = true)
//     |-- departifactid: array (nullable = true)
//     |    |-- element: string (containsNull = true)
//     |-- depgroupid: array (nullable = true)
//     |    |-- element: string (containsNull = true)
//     |-- depscope: array (nullable = true)
//     |    |-- element: string (containsNull = true)
//     |-- depversion: array (nullable = true)
//     |    |-- element: string (containsNull = true)
//     |-- modelversion: string (nullable = true)
//     |-- projectname: string (nullable = true)
//     |-- packaging: string (nullable = true)
//     |-- url: string (nullable = true)
//     |-- version: string (nullable = true)
//     |-- groupid: string (nullable = true)
//     |-- parentartifactid: string (nullable = true)
//     |-- parentgroupid: string (nullable = true)
//     |-- parentversion: string (nullable = true)
//    */
//    pomxmlhive.take(1).foreach(println)
//    //[HOSIUPortalDev,null,ArrayBuffer(junit, javax.servlet-api, spring-boot-starter-data-elasticsearch, spring-boot-starter, elasticsearch, spring-boot-starter-web, bootstrap, gson, tomcat-embed-jasper),ArrayBuffer(junit, javax.servlet, org.springframework.boot, org.springframework.boot, org.elasticsearch, org.springframework.boot, org.webjars, com.google.code.gson, org.apache.tomcat.embed),ArrayBuffer(test, null, null, null, null, null, null, null, provided),ArrayBuffer(3.8.1, 3.1.0, 1.2.5.RELEASE, 1.2.5.RELEASE, 1.4.1, null, 3.3.5, 2.4, null),4.0.0,HOSIUPortalDev Maven Webapp,war,http://maven.apache.org,0.0.1-SNAPSHOT,org.springframework.boot,spring-boot-starter-parent,org.springframework.boot,1.2.7.RELEASE]
//
//    //sqlContext.sql("dfs -rmr /user/horf/pomxmlhive")
//    //pomxmlhive.write.parquet("/user/horf/pomxmlhive")
//    //sqlContext.sql("drop table if exists imdp.pomxmlhive")
//    //sqlContext.sql("create table imdp.pomxmlhive (artifactid string,finalname string,departifactid array<string>,depgroupid array<string>, depscope array<string>,depversion array<string>,modelversion string,projectname string,packaging string,url string,version string,groupid string,parentartifactid string,parentgroupid string,parentversion string) stored as parquet")
//    //sqlContext.sql("load data inpath '/user/horf/pomxmlhive' into table imdp.pomxmlhive")

  }
}
