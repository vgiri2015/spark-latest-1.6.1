//import au.com.bytecode.opencsv.CSVParser
//import org.apache.spark.{SparkConf, SparkContext}
//
//object CSVParserPlus  {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("CASMaster").setMaster("local")
//    val sc = new SparkContext(conf)
//    val csvParser = new CSVParser(' ')
//    val input = sc.textFile("D:\\RnD\\Workspace\\scala\\TestSpark\\testData\\peoplefile.txt")
//    val maps = input.mapPartitions(parse)
//    }
//  }
//def parse(line: String,csvParser:CSVParser): Unit= {
//  csvParser.parseLine(line)
//  println("Parsing Success")
//}