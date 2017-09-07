package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.config.DBName
import com.proud.ark.data.HDFSUtil

/**
将著作权信息关联到企业
nohup spark-submit --driver-memory 25g --class com.proud.dc.handle.GuanlianZhuzuoquanToQiye --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
spark-submit --executor-memory 10g --master spark://bigdata01:7077 --class com.proud.dc.handle.GuanlianZhuzuoquanToQiye --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
 */

object GuanlianZhuzuoquanToQiye {
  case class ZhuzuoQuan(zhuzuoquan_id:Long, name:String)
  
  def main(args: Array[String]): Unit = {
    DBUtil.truncate(s"${DBName.bid}.company_ruanzhu")
	  val spark = SparkSession.builder().appName("GuanliZhuanliToQiye").master(ConfigUtil.master).config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  
	  val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "company_id")
	  val zhuzuoquanDF = DBUtil.loadDFFromTable("bid.ruanjian_zhuzuoquan", spark).select("id", "owner").filter(x => x.getAs[String]("owner") != null)
	  
	  val zhuzuoquan = zhuzuoquanDF.flatMap(row => {
	    val id = row.getAs[Int]("id")
	    row.getAs[String]("owner").split(";").map { x => ZhuzuoQuan(id, x) }
	  })
    
	  val result = zhuzuoquan.join(companyDF, "name").select("company_id", "zhuzuoquan_id")
	  DBUtil.saveDFToDB(result, s"${DBName.bid}.company_ruanzhu")
  }
	  
}