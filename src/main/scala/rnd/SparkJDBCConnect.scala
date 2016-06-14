package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Varatharajan Giri Ramanathan on 2/24/2016.
  */
object SparkJDBCConnect {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("SparkConnectJDBC")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val prop = new java.util.Properties
    //    val oracleJdbcUrl = s"jdbc:oracle:thin:SVCHDOOP/NEWPASSWORD1@P01DOL462D.ent.rt.csaa.com:1521/PASPRODDG"
    val oracleJdbcUrl = s"jdbc:oracle:thin:SVC_HORF/NEWPASSWORD99@P01DOL465.ent.rt.csaa.com:1521/bdwprep"
    val mysqlJdbcurl="jdbc:mysql://localhost:3306/hadoopdb"
    oracleJdbc(oracleJdbcUrl,sqlContext)
    mysqlJdbc(mysqlJdbcurl,sqlContext)

    //START --> ORACLE * JDBC * CONNECTION
    //Include ojdbc7.jar in the Dependency Window.
    //Below Example --> The data will be loaded into 10 partitions which means first partition will have records whose primary keys are within 1 to roughly 2000. And for each round trip, this will fetch 10 record at a time.
    def oracleJdbc(url:String, sqlContext: SQLContext) : Unit =
    {
      val tableName = s"BDW_POLICY_LND.T_SIS_ALSTMF"
      //    val fetchQuery = "(select ID,POLICYNUMBER from pasadm.policysummary where rownum <=100)"
      val fetchQuery = "select * from BDW_POLICY_LND.T_SIS_ALSTMF where rownum < 20"
      val tablePart = s"ID"
      val policySummary = sqlContext.read.format("jdbc")
        .options(
          Map("url" -> oracleJdbcUrl,
            "dbtable" -> fetchQuery, //Query Based Data Retrieval
            //  "dbtable" -> tableName, //Entire Table Based Data Retrieval
            "partitionColumn" -> tablePart,
            "lowerBound" -> "1",
            "upperBound" -> "2000",
            "numPartitions" -> "10",
            "fetchSize" -> "100")).load()
      policySummary.printSchema()
      policySummary.foreach(println)
      println(sqlContext.getAllConfs)
      //Below is another way to read Oracle through Spark JDBC
      //prop.setProperty("user","SVCHDOOP")
      //prop.setProperty("password","NEWPASSWORD1")
      //val policySummary = sqlContext.read.jdbc(oracleJdbcUrl,"pasadm.policysummary",prop)

      //Doing some ETL
      //val p1 = policySummary.where(policySummary("POLICYNUMBER")==="QCAAS200000001")
      //val p2 = policySummary.where(policySummary("POLICYNUMBER")==="QCAAS200000002")
      //val joined = p1.join(p2,p1("POLICYNUMBER")<=>p2("POLICYNUMBER")).foreach(println)
      //
      /*Writing the Output Into into Oracle
      case class Conf(id: String, policynumber: String)
      val data = sc.parallelize( Seq(Conf("1", "AZSS123456"), Conf("2", "CASS123456"), Conf("3", "NCSS123456"), Conf("4", "NYSS123456") ))
      import sqlContext.implicits._
      val df = data.toDF()
      df.write.jdbc(oracleJdbcUrl,"pasadm.policy",prop)
      df.write.insertInto("pasadm.policy")
      */
    }
    //END -- > ORACLE * JDBC * CONNECTION
    def mysqlJdbc(url:String,sQLContext: SQLContext) : Unit = {

    }
  }
}
