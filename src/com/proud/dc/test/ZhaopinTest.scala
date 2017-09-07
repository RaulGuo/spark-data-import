package com.proud.dc.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import com.proud.dc.util.Util
import com.proud.ark.config.DBName

object ZhaopinTest {
  def main(args: Array[String]): Unit = {
    val companyInfoTable = s"${DBName.jobs}.company_info"
    val jobInfoTable = s"${DBName.jobs}.company_employment"
    
    val spark = SparkSession.builder().appName("Test").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    
    DBUtil.truncate(companyInfoTable)
    //整理51job和智联的公司信息
    val comp51job = DBUtil.loadCrawlerData("crawler.51job_companys_info", 5, spark).select("company_name")
    println("---------------11111111111111111----------"+comp51job.count())
    Thread.sleep(1000*3)
    
    val zhilianCompDF = DBUtil.loadCrawlerData("crawler.zhilian_info_company", 5, spark).select("company_name")
    println("---------------22222222222222222222----------"+zhilianCompDF.count())
    Thread.sleep(1000*3)
    
    val jobCompDF = DBUtil.loadCrawlerData("jobs.job_company", 5, spark).select("company_name")
    
    println("---------------3333333333333333333333333----------"+jobCompDF.count)
    Thread.sleep(1000*3)
  }
}