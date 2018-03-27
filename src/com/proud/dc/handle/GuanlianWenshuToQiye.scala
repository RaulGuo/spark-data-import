package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.storage.StorageLevel
import com.proud.dc.util.Util
import com.proud.ark.config.DBName
import com.proud.ark.data.HDFSUtil

/**
 * 将文书跟企业ID关联起来。文书跟企业之间是多对多的关系
 * nohup spark-submit --driver-memory 25g --class com.proud.dc.handle.GuanlianWenshuToQiye --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
 * spark-submit --executor-memory 10g --master spark://bigdata01:7077 --class com.proud.dc.handle.GuanlianWenshuToQiye --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 * 
 * 统计企业的涉诉案由以及数量（不分时间）
 */

object GuanlianWenshuToQiye {
  
  case class Wenshu(id:Long, name:String)
  
  case class ShesuFenxi(company_id:Long, year:Int)
  
  def main(args: Array[String]): Unit = {
    val resultTable = s"${DBName.wenshu}.wenshu_company"
    DBUtil.truncate(resultTable)
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanliWenshuToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse)
	  .master(ConfigUtil.master).getOrCreate()
	  
	  val sc = spark.sparkContext
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  //获取企业信息，删除不规范的企业
	  val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "company_id").filter(x => {
	    val name = x.getAs[String]("name")
	    !(name == null || name.trim().isEmpty() || name.contains("＊＊") || name.contains("*"))
	  })
	  
	  val wenshu = DBUtil.loadDFFromTable("wenshu.ws_content_primary", 4, spark).select("id", "appellor")
	  .filter(x => x.getAs[String]("appellor") != null).flatMap(x => {
	    val id = x.getAs[Int]("id")
	    x.getAs[String]("appellor").split(",").map { x => Wenshu(id, x) }
	  })
	  
	  val wsCompResult = wenshu.join(companyDF, "name").select("id", "company_id")
	  DBUtil.saveDFToDB(wsCompResult, resultTable)
	  
//	  //统计企业的不同的涉诉案由以及总数量
//	  val anyouDF = DBUtil.loadDFFromTable("wenshu.ws_content_primary", spark).select("id", "caseReason").join(wsCompResult, "id").select("company_id", "caseReason")
//	  val anyouShuliangDF = anyouDF.groupBy("company_id", "caseReason").count().withColumnRenamed("count", "number")
//	  DBUtil.saveDFToDB(anyouShuliangDF, s"${DBName.wenshu}.qiye_shesu_anyou_tongji")
//	  
//	  //统计企业的涉诉分析：
//	  val riqiDF = DBUtil.loadDFFromTable("wenshu.ws_content_primary", spark).select("id", "date").join(wsCompResult, "id").select("company_id", "date").map(x => {
//	    ShesuFenxi(x.getAs("company_id"), Util.getYearFromDate(x.getAs("date")))
//	  }).groupBy("company_id", "year").count().withColumnRenamed("count", "number")
//	  DBUtil.saveDFToDB(anyouShuliangDF, s"${DBName.wenshu}.qiye_shesu_fensu")
	  
	  
  }
  
  
 
  
}