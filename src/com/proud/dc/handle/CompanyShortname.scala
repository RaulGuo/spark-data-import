package com.proud.dc.handle

import com.proud.ark.db.DBUtil
import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.data.HDFSUtil

/**
 * 计算企业的简称
 * 
 */

object CompanyShortname {
  val spark = SparkSession.builder().appName("CompanyShortname").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).config("master", ConfigUtil.master).getOrCreate()
  def main(args: Array[String]): Unit = {
    val result = "dc_import.company_shortname"
    DBUtil.truncate(result)
    val data = "tag.company_short_name"
    val df = DBUtil.loadDFFromTable(data, spark)//name, short
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "company_id")//company_id, name
    
    val resultDF = df.join(companyDF, "name").withColumnRenamed("short", "short_name").select("company_id", "short_name")
    DBUtil.saveDFToDB(resultDF, result)
  }
}