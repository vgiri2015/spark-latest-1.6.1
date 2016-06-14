package rnd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gfp2ram on 1/30/2016.
  */
object EsToHiveRead {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
    val sparkconf = new SparkConf()
      .setAppName("EStoHiveSpark")
      .setMaster("local[*]")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")

    //This Config is Working correctly.
    val esConfig = Map(("es.nodes","localhost"),("es.port","9200"),("es.index.auto.create","false"),
      ("es.read.field.empty.as.null","true"),("es.http.timeout", "5m"),("es.mapping.id","CasDsKey"))

    //*This Configuration is not working
    //    val esConfig = Map(("es.nodes","localhost"),("es.port","9200"),("es.index.auto.create","false"),
    //      ("es.read.field.empty.as.null","true"),("es.http.timeout", "5m"),("es.read.field.as.array.include","VendorInfo.VendorAddress," +
    //        "VendorInfo.VendorAlternativeEmail,VendorInfo.VendorCellphone,VendorInfo.VendorDescription,VendorInfo.VendorEmail," +
    //        "VendorInfo.VendorHomePhone,VendorInfo.VendorName,VendorInfo.VendorRole,VendorInfo.VendorTaxId," +
    //        "VendorInfo.VendorWorkPhone"),("es.mapping.id","CasDsKey"))

    val sc = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)
    val horfesdfJson=sqlContext.read.json("C:\\Users\\gfp2ram\\Desktop\\wip\\horf_mar15_index.json")
    //horfesdfJson.toJSON.take(1).foreach(println)
    import org.elasticsearch.spark.sql._
    horfesdfJson.saveToEs("horfindex_mar15/rfdata",cfg = esConfig)
    /*
    val readESindex = sqlContext.read.format("org.elasticsearch.spark.sql").options(esConfig).load("hoindex_mar15/rfdata")
    readESindex.registerTempTable("readHorfEsIndex")
    val horfHivedf = sqlContext.sql("select AcceptCode,	AdjustorComments,AgeoftheClaimant,AgeoftheInsured,AssignedDate," +
      "AssignmentStatus,AvgPolicyScore,BaseYear,CatastropheCodeInternal,CatastrophecodePCS,Catastrophydescription," +
      "CauseOfLoss,ClaimAdjustor,ClaimAdjustorJobGroup,ClaimantAddress,ClaimantAlternativeEmail,ClaimantCity," +
      "ClaimantDateofBirth,ClaimantEmail,ClaimantHomePhone,ClaimantMobilePhone,ClaimantName,ClaimantWorkPhone," +
      "ClaimantZipCode,ClaimCloseDate,ClaimDescription,ClaimNumber,ClaimOpenDate,ClaimStatus,CoverageLimit," +
      "Coverages,CovsExposure,CumulativeExpense,CumulativePayment,DateOfLoss,DaysBwtPincandLossdate,Deductible," +
      "EmailNotification,Expenseamt,Exposure,ExposureAdjustor,ExposureAdjustorJobGroup,ExposureCloseDate," +
      "ExposureOpenDate,ExposureOutcome,ExposureStatus,HiveLoadDate,HOModelScore,InsuredAddress," +
      "InsuredBirthDate,InsuredCity,InsuredEmail,InsuredHomePhone,InsuredMobilePhone,InsuredName," +
      "InsuredWorkPhone,InsuredZipcode,ISOClaimNumber,ISOFileNumber,ISOKeyIndicators,LossSubtype," +
      "LossType,MembershipNumber,NumClaimants,NumClaimPerPolicy,NumdaysClmExpOpndtgrtDol365," +
      "ClmExpOpndtgrtDol365Indicator,NumFlagClaim,NumFlagPerPolicy,NumISOFlagClaimSiuInvolved," +
      "PaymentAmt,Persistency,PolicyAppliedCovDed,PolicyAppliedCovDedLastChangeAmt," +
      "PolicyAppliedCovDedLastChangeDate,PolicyAppliedCovFirstBegEffDate," +
      "PolicyAppliedCovLimit,PolicyAppliedCovLimitLastChangeAmt,PolicyAppliedCovLimitLastChangeDate," +
      "PolicyCancelLapseReason,PolicyEffectiveDate,PolicyExpiryDate,PolicyInceptionDate,PolicyIssuingAgent," +
      "PolicyNumber,PolicyNumTimesReinstated,PolicyNumTimesRenewed,PolicyScore,PolicySourceSystem,PolicyState," +
      "PolicyStatus,PolicyType,PotentialSavings,PremiseAddress,PriorClmSIU,RF10Indicator," +
      "RF10NumDaysClmReptoDOL,RF11Indicator,RF11NumDaysCatOccrtoDOL,RF12Indicator," +
      "RF12NumISOSIUInvolvedPolicy,RF13Indicator,RF13NumISOKeyIndClm,RF14Indicator," +
      "RF14NumISONICBForewarnPolicy,RF15Indicator,RF15NumISODeathMasterHitPolicy5yrs,RF16Indicator," +
      "RF16NumISOMailDropPolicy,RF17Indicator,RF17NumISOLossLocndiff,RF18Indicator,RF18NumOfCnclNotmnths," +
      "RF19Indicator,RF19NumOfReinst36mnths,RF1Indicator,RF1NumDaysDeducchange,RF20Indicator," +
      "RF20NumPolicySamePhone,RF21Indicator,RF21NumJewlClmsLst5yrs,RF22Indicator,RF22NumArtClmsLst5yrs," +
      "RF23HOClaimsPerPolicylst5yrs,RF23Indicator,RF24Indicator,RF24NumTHEFTClaimlst5yrs,RF25Indicator," +
      "RF25NumFIREClaim5yrs,RF26Indicator,RF26NumWATERClaimlst5yrs,RF27Indicator,RF27NumVANClaimlst5yrs," +
      "RF28Indicator,RF28NumHAILClaimlst5yrs,RF29Indicator,RF29NumMOLDClaims5yrs,RF2Indicator," +
      "RF2NumdaysbtwnIncepDOL,RF30Indicator,RF30NumCSAASIUInvolvedPolicy,RF3Indicator," +
      "RF3NumDaysMatchCovChangeDOL,RF4Indicator,RF4NumDaysMatchCovAddDOL,RF5Indicator,RF5NumDaysJewlEndorseAddDOL," +
      "RF6Indicator,RF6NumDaysArtEndorseAddDOL,RF7Indicator,RF7NumDaysCancNotDOL,RF8Indicator,RF8NumdaysPolReinstDOL,RF9Indicator," +
      "RF9NumdaysnonrenDOL,Roles,SIUAssignedBy,SIUAssignedTo,SIUEvaluator,SiuPoints,State,TotalSIUScoreClaim,VendorInfo," +
      "CasDsKey from readHorfEsIndex")
      horfHivedf.take(50).foreach(println)
      */
  }
}
