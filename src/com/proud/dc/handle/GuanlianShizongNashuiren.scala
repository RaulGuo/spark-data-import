package com.proud.dc.handle

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.GlobalVariables
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.SaveMode

/**
 * 关联失踪纳税人和企业，计算企业是否数据失踪纳税人，以及对应的失踪纳税人的信息。
 */
object GuanlianShizongNashuiren {
  def main(args: Array[String]): Unit = {
    val resultTable = "tax.shigzong_company"
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanlianShizongNashuiren").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    
    val df = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsCompanyDir, spark).select("id", "name").withColumnRenamed("id", "company_id")
    val shizong = DBUtil.loadDFFromTable("tax.shizong_nashuiren_qsr", spark).select("id", "taxpayer_name").withColumnRenamed("id", "shizong_id")
    
    val result = df.join(shizong, $"name" === $"taxpayer_name").select("company_id", "shizong_id")
    DBUtil.truncate(resultTable)
    DBUtil.saveDFToDB(result, resultTable)
    
  }
}