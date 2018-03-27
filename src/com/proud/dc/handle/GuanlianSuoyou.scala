package com.proud.dc.handle

/**
 * 
 * 将多个企业相关的对象进行关联
 */
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.data.HDFSUtil
import org.apache.commons.codec.digest.DigestUtils
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.SaveMode

object GuanlianSuoyou {
  case class Result(company_id:Long, company_md5:String)
  def main(args: Array[String]): Unit = {
	  val spark = SparkSession.builder().appName("GuanliZhuanliToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
	  val sc = spark.sparkContext
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  
	  val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).withColumnRenamed("id", "company_id")
	  val df = DBUtil.loadDFFromTable("dc_import.company", 60, spark).select("id", "reg_no", "type")
	  //过滤掉个体户以及没有注册号的企业，删除掉有重复注册号的记录。
    val filter = df.filter { x => {
      val tp = x.getAs[String]("type")
      val regNo = x.getAs[String]("reg_no")
      (tp == null || !tp.contains("个体")) && (regNo != null && !regNo.trim.isEmpty())
    } }.dropDuplicates("reg_no")
    
    val result = filter.map { x => {
    Result(x.getAs("id"), DigestUtils.md5Hex(x.getAs[String]("reg_no")))
    } }
    
    val idMd5 = DBUtil.loadDFFromTable("statistics.company_id_md5", 100, spark).select("company_id", "company_md5").persist()

    GlobalVariables.provinces.foreach { p => {
      try{
        val df = DBUtil.loadDFFromTable("gis_info."+p+"_company_gis_info", 10, spark).select("city", "district", "latitude", "longitude", "text", "company_id")
        val tmpGis = idMd5.join(df, "company_id").select("city", "district", "latitude", "longitude", "text", "company_md5").dropDuplicates("company_md5")
        DBUtil.insertIgnoreDFToDB(tmpGis, "gis_info.company_gis_info", SaveMode.Append);
      }catch{
        case e:Exception => {
          println(p+" province import error");
          e.printStackTrace()
        }
      }
    } }
    
    DBUtil.saveDFToDB(result, "statistics.company_id_md5");
  }
}