package learning

//Question# 1 -- > Log Parsing

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object logParsing {
  case class logr(ipAddress: String,clientIdentity: String,userName: String,dateTime: String,kindofreq: String,statecode1: String,statecode2: String,referer: String,browser: String)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("LogParse").setMaster("local")
    val sc = new SparkContext(conf)
    val sCtx = new SQLContext(sc)
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val browser = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $browser".r
//    def parse_logs(logfile:RDD[String]) : Row =  {
//      val pattern = new Pattern(regex)
//      val matcher = pattern.matcher(logfile.toString())
//      return Row(matcher.group(0),matcher.group(1))
//    }
//    val rawLogFile = sc.textFile("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\testSpark_log")

  }



}
//display(logFileOutput)

//Am getting ArrayOutofBound Exception somehow I could not fix this due to time constraints


//Generating a list of URLs from Apache access log files, sorted by hit count
//Analyzing Apache access logs with Spark and Scala


//'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

