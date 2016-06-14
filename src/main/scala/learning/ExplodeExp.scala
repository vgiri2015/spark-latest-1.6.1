package learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gfp2ram on 11/5/2015.
 */
object ExplodeExp {
  def main(args:Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val conf = new SparkConf().setAppName("GroupByTesting").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val input = sc.textFile("D:\\SVNCodeBase\\HORF\\dev\\coding\\02.FlatDP\\SIUDataPipeline\\src\\test\\testData\\hdesSampleFiles\\amastcp2_v1.txt")
    val masSchema ="policy_number,policy_status_code,first_policy_eff_date,policy_inception_date,policy_expiration_date,policy_status_date,count_cancel_this_yr,count_cancel_last_yr,num_times_renewed,coverage,cancel_date,cancel_reason,oldest_insured_dob,lst_name1,fst_name1,mid_name1,pre_name1,suf_name1,lst_name2,fst_name2,mid_name2,pre_name2,suf_name2,lst_name3,fst_name3,mid_name3,pre_name3,suf_name3,lst_name4,fst_name4,mid_name4,pre_name4,suf_name4,lst_name5,fst_name5,mid_name5,pre_name5,suf_name5,insured_address1_1st,insured_address2_1st,insured_address3_1st,insured_city_1st,insured_state_1st,zip_code_address_1st,premises_address1_1st,premises_address2_1st,premises_address3_1st,premises_city_1st,premises_state_1st,premises_zip_code,telephone_no,alt_telephone_no,total_annual_prem,total_actual_prem,form_type,membership_no,rep_d_o,rep_no,create_ts,row_num_id"
    val masSchema_s = StructType(masSchema.split(",").map(f => StructField(f, StringType, true)))
    case class coverageCodes(coverage:Seq[String])
    val input_row = input.map(_.split("<-->")).map(r => org.apache.spark.sql.Row(
      r(0)+""+r(1)+""+r(2).substring(0,2)+""+r(3).substring(0,1),r(4),r(5),r(6),r(7),r(8),r(9),r(10),r(11),
      "COVA:"+r(12)::"COVB:"+r(13)::"COVC:"+r(14)::"COVD:"+r(15)::"COVE:"+r(16)::"COVF:"+r(17)::"COVF_Acc:"+r(18)::Nil,
      r(19),r(20),r(21),r(22),r(23),r(24),r(25),r(26),r(27),r(28),r(29),r(30),r(31),r(32),r(33),r(34),r(35),r(36),r(37),
      r(38),r(39),r(40),r(41),r(42),r(43),r(44),r(45),r(46),r(47),r(48),r(49),r(50),r(51),r(52),r(53),r(54),r(55),r(56),
      r(57),r(58),r(59),r(60),r(61),r(62),r(63),r(64),r(65),r(66),r(67),r(68)))
      val inputDF = sqlContext.createDataFrame(input_row,masSchema_s)
      val inputSelect = inputDF.explode("coverage","covsplit"){coverage:String => coverage.split(",")}
      val newEx= inputSelect.select("policy_number","policy_status_code","first_policy_eff_date","policy_inception_date","policy_expiration_date","policy_status_date","count_cancel_this_yr","count_cancel_last_yr","num_times_renewed","covsplit","cancel_date","cancel_reason","oldest_insured_dob","lst_name1","fst_name1","mid_name1","pre_name1","suf_name1","lst_name2","fst_name2","mid_name2","pre_name2","suf_name2","lst_name3","fst_name3","mid_name3","pre_name3","suf_name3","lst_name4","fst_name4","mid_name4","pre_name4","suf_name4","lst_name5","fst_name5","mid_name5","pre_name5","suf_name5","insured_address1_1st","insured_address2_1st","insured_address3_1st","insured_city_1st","insured_state_1st","zip_code_address_1st","premises_address1_1st","premises_address2_1st","premises_address3_1st","premises_city_1st","premises_state_1st","premises_zip_code","telephone_no","alt_telephone_no","total_annual_prem","total_actual_prem","form_type","membership_no","rep_d_o","rep_no","create_ts","row_num_id")
      newEx.rdd.saveAsTextFile("D:\\SVNCodeBase\\HORF\\dev\\coding\\02.FlatDP\\SIUDataPipeline\\src\\test\\testData\\hdesSampleFiles\\expout")
  }
}
