package com.proud.ic.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil

/**
 spark-submit --master spark://bigdata01:7077 --executor-memory 14G --class com.proud.ic.handle.ReduceWenshu --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */

object ReduceWenshu {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GongkongXinxi").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).enableHiveSupport().getOrCreate()
    import spark.implicits._
    val companyIdDF = DBUtil.loadDFFromTable("dc_import.zhejiang_company",16, spark).select("id").withColumnRenamed("id", "zhejiang_id")
    val wenshuCompanyDF = DBUtil.loadDFFromTable("wenshu.wenshu_company", 16, spark)
    
    val result = companyIdDF.join(wenshuCompanyDF, $"zhejiang_id" === $"company_id").select("id", "company_id")
//    DBUtil.saveDFToICDB(result, "wenshu.wenshu_company_1")
    
    val wsIds = result.select("id").withColumnRenamed("id", "ws_id").distinct().persist()
    DBUtil.saveDFToICDB(wsIds, "wenshu.ws_ids_1")
    
//    val wsIds = DBUtil.loadDFFromICTable("wenshu.ws_ids", spark).withColumnRenamed("id", "ws_id").distinct().persist()
    
//    DBUtil.truncateIC("wenshu.ws_content_primary")
//    for(i <- 0 to 50){
//      val wsPrimaryDF = DBUtil.loadDFFromTableWithPredicate("wenshu.ws_content_primary", i, spark)
//      val wsFilterDF = wsPrimaryDF.join(wsIds, $"id" === $"ws_id").drop("ws_id")
//      DBUtil.saveDFToICDB(wsFilterDF, "wenshu.ws_content_primary")
//    }
    
//    DBUtil.truncateIC("wenshu.ws_content_attachment")
//    for(i <- 0 to 50){
//      val wsPrimaryDF = DBUtil.loadDFFromTableWithPredicate("wenshu.ws_content_attachment", i, spark)
//      val wsFilterDF = wsPrimaryDF.join(wsIds, $"id" === $"ws_id").drop("ws_id")
//      DBUtil.saveDFToICDB(wsFilterDF, "wenshu.ws_content_attachment")
//    }
//    
//    val zhuanliIds = DBUtil.loadDFFromICTable("patent.zhuanli_quanren", 16, spark).select("zhuanli_id").distinct()
//    DBUtil.truncateIC("patent.zhuanli_xinxi")
//    for(i <- 0 to 40){
//      val wsPrimaryDF = DBUtil.loadDFFromTableWithPredicate("patent.zhuanli_xinxi", i, spark)
//      val wsFilterDF = wsPrimaryDF.join(zhuanliIds, $"id" === $"zhuanli_id").drop("zhuanli_id")
//      DBUtil.saveDFToICDB(wsFilterDF, "patent.zhuanli_xinxi")
//    }
  }
}