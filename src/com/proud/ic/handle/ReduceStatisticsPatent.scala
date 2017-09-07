package com.proud.ic.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil

/**
 spark-submit --master spark://bigdata01:7077 --executor-memory 14G --class com.proud.ic.handle.ReduceStatisticsPatent --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */

object ReduceStatisticsPatent {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GongkongXinxi").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).enableHiveSupport().getOrCreate()
    import spark.implicits._
    val companyIdDF = DBUtil.loadDFFromTable("dc_import.zhejiang_company",32, spark).select("id").withColumnRenamed("id", "zhejiang_id").persist()
    
    val chigubili = DBUtil.loadDFFromTable("statistics.chigu_bili", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("statistics.chigu_bili")
    DBUtil.saveDFToICDB(chigubili, "statistics.chigu_bili")
      
    val invRelatioship = DBUtil.loadDFFromTable("statistics.company_investment_relationship", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("statistics.company_investment_relationship")
    DBUtil.saveDFToICDB(invRelatioship, "statistics.company_investment_relationship")
    
    val invDistribute = DBUtil.loadDFFromTable("statistics.company_invest_district_distribute", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("statistics.company_invest_district_distribute")
    DBUtil.saveDFToICDB(invDistribute, "statistics.company_invest_district_distribute")
    
    val relatedCount = DBUtil.loadDFFromTable("statistics.company_related_count", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("statistics.company_related_count")
    DBUtil.saveDFToICDB(relatedCount, "statistics.company_related_count")
    
    val stockRela = DBUtil.loadDFFromTable("statistics.company_stock_relationship", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("statistics.company_stock_relationship")
    DBUtil.saveDFToICDB(stockRela, "statistics.company_stock_relationship")
    
    val zhuanliGongsi = DBUtil.loadDFFromTable("patent.zhuanli_gongsi_xinxi", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("patent.zhuanli_gongsi_xinxi")
    DBUtil.saveDFToICDB(zhuanliGongsi, "patent.zhuanli_gongsi_xinxi")
    
    val zhuanliQuanren = DBUtil.loadDFFromTable("patent.zhuanli_quanren", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("patent.zhuanli_quanren")
    DBUtil.saveDFToICDB(zhuanliQuanren, "patent.zhuanli_quanren")
    
//    val zhuanliXinxi = DBUtil.loadDFFromTable("patent.zhuanli_xinxi", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
//    DBUtil.truncateIC("patent.zhuanli_xinxi")
//    DBUtil.saveDFToICDB(zhuanliXinxi, "patent.zhuanli_xinxi")
    
    val empDF = DBUtil.loadDFFromTable("jobs.company_employment", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("jobs.company_employment")
    DBUtil.saveDFToICDB(empDF, "jobs.company_employment")
    
    val companyInfo = DBUtil.loadDFFromTable("jobs.company_info", 32, spark).join(companyIdDF, $"company_id" === $"zhejiang_id").drop("zhejiang_id")
    DBUtil.truncateIC("jobs.company_info")
    DBUtil.saveDFToICDB(companyInfo, "jobs.company_info")
  }
}