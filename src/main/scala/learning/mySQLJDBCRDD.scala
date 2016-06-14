package learning

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gfp2ram on 9/6/2015.
 */
object mySQLJDBCRDD {
  def main(args: Array[String]): Unit = {
    val resultset: ResultSet = null
    createmySQLConnection()
    extractValues(resultset)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("HiveConnect").setMaster("local")
    val sc = new SparkContext(conf)
    val data = new JdbcRDD(sc, createmySQLConnection, "SELECT * FROM USER", lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  def createmySQLConnection() = {
    Class.forName("com.jdbc.mysql.Driver").newInstance()
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?"+"user=gfp2ram&password=gfp2ram")
  }

  def extractValues(r: ResultSet): Unit = {
    (r.getString(1), r.getString(2))
  }
}
//    def createmySQLConnection(): Unit = {
//      val driver = "com.mysql.jdbc.Driver"
//      val url = "jdbc://mysql://localhost:3306/test"
//      val username = "gfp2ram"
//      val password = "gfp2ram"
//      var connection: Connection = null
//      try {
//        Class.forName(driver)
//        val connection = DriverManager.getConnection(url, username, password)
//        val statement = connection.createStatement()
//        val resultSet = statement.executeQuery("SELECT host, user FROM user")
//        while (resultSet.next()) {
//          val host = resultSet.getString("host")
//          val user = resultSet.getString("user")
//          println("host, user = " + host + ", " + user)
//        }
//      } catch {
//        case e => e.printStackTrace()
//      }
//      connection.close()
//    }
//  }
//}
