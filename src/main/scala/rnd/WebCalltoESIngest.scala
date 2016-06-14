package rnd

import java.io.File
import java.net.URL

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gfp2ram on 3/4/2016.
  */
object WebCalltoESIngest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("JsonToEsLoad")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //ElasticSearch Configurations and Input Path to the File.
    val hollyWoodFile = new File("D:\\RnD\\Workspace\\scala\\SparkLatest\\src\\test\\holooywood.json")
    val esConfig = Map(("es.nodes", "localhost"), ("es.port", "9200"), ("es.index.auto.create", "true"), ("es.http.timeout", "5m"))

    //If File Exists Already, Delete it Recreate it.
    if (hollyWoodFile.exists()) {
      println("File Already Exists in So Deleting..." + hollyWoodFile.getAbsolutePath)
      hollyWoodFile.delete()
      jsontoEsLoad(sqlContext: SQLContext, hollyWoodFile,esConfig)
    }

  }
  //Recreate the File and Save to Elastic Search
  def jsontoEsLoad(sqlContext: SQLContext, hollyWoodFile: File,esConfig:Map[String,String]) : Unit ={

    //URL Download of Json to Local File. Here you can use any file System. Ex. LocalFs, HDFS,S3,etc
    FileUtils.copyURLToFile(new URL("http://www.omdbapi.com/?t=iron+man&y=&plot=full&r=json?accessType=DOWNLOAD"), hollyWoodFile)
    val jsonDf = sqlContext.read.json(hollyWoodFile.getAbsolutePath)

    //Storing it in Elastic Search Dynamically
    import org.elasticsearch.spark.sql._
    jsonDf.saveToEs("hollywood/movie", cfg = esConfig)

    //Read the jsonFile as a DataFrame and Select Some values
    jsonDf.registerTempTable("ironman")
    sqlContext.sql("select Actors,Director,Year,Title from ironman").take(1).foreach(println)
    /*
    16/03/04 22:57:34 INFO DAGScheduler: ResultStage 1 (take at JsonToEsLoad.scala:61) finished in 0.347 s
      16/03/04 22:57:34 INFO DAGScheduler: Job 1 finished: take at JsonToEsLoad.scala:61, took 0.358971 s
    [Robert Downey Jr., Terrence Howard, Jeff Bridges, Gwyneth Paltrow,Jon Favreau,2008,Iron Man]
    */

    //Read the Elastic Search Index and Display Some Records
    val readIronManIndex = sqlContext.read.format("org.elasticsearch.spark.sql").options(esConfig).load("hollywood/movie")
    readIronManIndex.registerTempTable("readIronManIndex")
    sqlContext.sql("select Actors,Director,Year,Title from readIronManIndex").take(1).foreach(println)
    /*
      16/03/04 22:59:52 INFO DAGScheduler: ResultStage 3 (take at JsonToEsLoad.scala:69) finished in 0.059 s
      16/03/04 22:59:52 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool
      16/03/04 22:59:52 INFO DAGScheduler: Job 3 finished: take at JsonToEsLoad.scala:69, took 0.066117 s
    [Robert Downey Jr., Terrence Howard, Jeff Bridges, Gwyneth Paltrow,Jon Favreau,2008,Iron Man]
    */



  }
}
