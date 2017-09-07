package com.proud.dc.handle

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.GlobalVariables
import com.proud.ark.db.DBUtil

/**
 * 关联产业链中的企业信息：
 * 产业链的原始数据中包含企业的名称跟产业链环节的名称，
 * 需要将企业名称跟企业id关联，将产业链环节跟产业链环节的ID关联

 */

object GuanlianCompanyChain {
  def main(args: Array[String]): Unit = {
    val resultTable = "chain.company_industry_chain"
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanliWenshuToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    
    val df = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsCompanyDir, spark).select("id", "name").withColumnRenamed("id", "company_id")
    val chain = DBUtil.loadDFFromTable("chain.for_import", spark).select("company_name", "link_name").distinct()
    
    val category = DBUtil.loadDFFromTable("chain.industry_category", spark).select("id", "name").withColumnRenamed("id", "cate_id").withColumnRenamed("name", "cat_name")
    
    val result = df.join(chain, $"name" === $"company_name").join(category, $"link_name" === $"cat_name").select("company_id", "cate_id")
    DBUtil.truncate(resultTable)
    DBUtil.saveDFToDB(result, resultTable)
  }
}