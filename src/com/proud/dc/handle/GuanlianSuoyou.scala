package com.proud.dc.handle

/**
 * 将多个企业相关的对象进行关联
 */
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.data.HDFSUtil

object GuanlianSuoyou {
  
  def main(args: Array[String]): Unit = {
	  val spark = SparkSession.builder().appName("GuanliZhuanliToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
	  val sc = spark.sparkContext
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  
	  val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).withColumnRenamed("id", "company_id")
  }
}