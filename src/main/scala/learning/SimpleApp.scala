package learning

/**
 * Created by gfp2ram on 9/3/2015.
 */

//Download this -- > http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe
//I Manually downloaded hadoop-common package binaries from https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip and Placed in C:\\hadoop

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    //Below is the System Environment variable corresponding to HADOOP_HOME
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val logFile = "C:\\spark\\spark-1.4.1-bin-hadoop2.6\\README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    ////    val numAs = logData.filter(line => line.contains("a")).count()
    ////    val numBs = logData.filter(line => line.contains("b")).count()
    ////    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    //  }
  }
}