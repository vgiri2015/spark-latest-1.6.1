package learning

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

//References
//http://alvinalexander.com/scala/xml-parsing-xpath-extract-xml-tag-attributes
//http://programmingstories.blogspot.com/2014/12/read-large-gpx-or-xml-files-using.html
//https://github.com/HyukjinKwon/spark-xml/raw/master/src/test/resources/books.xml

/**
 * Created by gfp2ram on 11/4/2015.
 */
object XMLParser {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("XMLParsing").setMaster("local")
    val sc = new SparkContext(conf)

    //OPTION 1
//    val weather =
//      <rss>
//        <channel>
//          <title>Yahoo! Weather - Boulder, CO</title>
//          <item>
//            <title>Conditions for Boulder, CO at 2:54 pm MST</title>
//            <forecast day="Thu" date="10 Nov 2011" low="37" high="58" text="Partly Cloudy"
//                      code="29" />
//          </item>
//        </channel>
//      </rss>
//    val forecast = weather \ "channel" \ "item" \ "forecast"
//    val day = forecast \ "@day"     // Thu
//    val date = forecast \ "@date"   // 10 Nov 2011
//    val low = forecast \ "@low"     // 37
//    val high = forecast \ "@high"   // 58
//    val text = forecast \ "@text"   // Partly Cloudy
//    println(" "+day+" "+date+" "+low+" "+high+" "+text)


    //OPTION 2
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class",
      "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<trkseg")
    jobConf.set("stream.recordreader.end", "</trkseg>")
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, "D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\xmlinput.xml")

    // Load documents (one per line).
    val documents = sc.hadoopRDD(jobConf,	classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    import scala.xml.XML
    val texts = documents.map(_._1.toString)
      .map{ s =>
        val xml = XML.loadString(s)
        val trackpts = xml \ "trkpt"
        val gpsData = trackpts.map(
          xmlNode =>(
            (xmlNode \ "@lat").text.toDouble,
            (xmlNode \ "@lon").text.toDouble
          ))
        gpsData.toList
      }
    println(texts.first)

    //OPTION 3 -- > Under Construction in GitHub
//    import org.apache.spark.sql.SQLContext
//
//    val sqlContext = new SQLContext(sc)
//    val df = sqlContext.read
//    .format("org.apache.spark.sql.xml")
//    .option("rootTag", "trkseg") // This should be always given.
//    .load("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\books.xml")
//    df.collect().foreach(println)


    //OPTION 4 --> Under Construction in GitHub
//    import org.apache.spark.sql.SQLContext
//    import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
//
//    val sqlContext = new SQLContext(sc)
//    val customSchema = StructType(
//    StructField("author", StringType, nullable = true),
//    StructField("description", StringType, nullable = true),
//    StructField("genre", StringType ,nullable = true),
//    StructField("id", StringType, nullable = true),
//    StructField("price", DoubleType, nullable = true),
//    StructField("publish_date", StringType, nullable = true),
//    StructField("title", StringType, nullable = true))
//
//    val df = sqlContext.read
//    .format("org.apache.spark.sql.xml")
//    .option("rootTag", "book") // This should be always given.
//    .schema(customSchema)
//    .load("books.xml")
//
//    df.select("author", "id").collect().foreach(println)
  }
}
