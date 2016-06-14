package learning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by gfp2ram on 1/26/2016.
 */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val rep: RDD[String] = sc.textFile("/user/horf/orctotxt/hdes/initial/repname/2016-1-21")

    def schemaDef(baseSchema:String) : StructType = {
      return StructType(baseSchema.split(",").map(f => StructField(f, StringType, true)))
    }

    val dedDFdistinct = sqlContext.sql("select distinct identifier ded_table_name,policy_number as ded_policy_number,cov_limit as ded_deductible,cov_eff_dt as ded_eff_dt,cov_exp_dt as ded_exp_dt,cov_addt_dt as ded_addt_dt,endo_seq_no as ded_endo_seq_no,endo_rate_code as ded_rate_code,endors_identifier as ded_identifier,active_policy_pickup_dt as ded_active_policy_pickup_dt,hive_load_date as ded_hive_load_date from hdesstg.deductible")
    dedDFdistinct.cache()

    //repname File Split up
    val rep_bc = sc.broadcast(rep)
    val rep_schema = "rep_rep_do_no,rep_rep_no,rep_rep_name,rep_create_ts,rep_row_num_id"
    val rep_s = schemaDef(rep_schema)
    val rep_row = rep_bc.value.map(_.split("<-->")).coalesce(10).map(r => org.apache.spark.sql.Row(r(0), r(1), r(2), r(3), r(4)))
    val repDF = sqlContext.createDataFrame(rep_row, rep_s)
    repDF.registerTempTable("repDF")

    val masHistTmp1 = sqlContext.read.parquet("/user/horf/masHistTmp1Selected_jan25")
    masHistTmp1.registerTempTable("masHistTmp1")

    val ded_window1 = Window.partitionBy(dedDFdistinct("ded_policy_number")).orderBy(dedDFdistinct("ded_active_policy_pickup_dt").desc)
    val ded_windw2 = ded_window1.rowsBetween(Long.MinValue,0)
  }
}
