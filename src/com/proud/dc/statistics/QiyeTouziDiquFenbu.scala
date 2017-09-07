package com.proud.dc.statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import com.google.gson.Gson
import org.apache.spark.storage.StorageLevel
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.config.DBName

/**
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
spark-submit --class com.proud.dc.statistics.QiyeTouziDiquFenbu --executor-memory 10g --master spark://bigdata01:7077 --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */

object QiyeTouziDiquFenbu {
  def main(args: Array[String]): Unit = {
    val table = s"${DBName.statistics}.company_invest_district_distribute"
    DBUtil.truncate(table)
    
    val spark = SparkSession.builder().appName("QiyeTouziDiquFenbu").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    
    import spark.implicits._
    
    //src_id, dst_id
    val invDF = DBUtil.loadDFFromTable("dc_import.company_investment_enterprise", spark).select("src_id", "dst_id")
    invDF.createOrReplaceTempView("invest")
    
    //company_id, province
    val compProvinceDF = DBUtil.loadDFFromTable("dc_import.company", spark).select("id", "province").withColumnRenamed("id", "company_id")
    compProvinceDF.createOrReplaceTempView("comp_province")
    
    //code, province
    val provinceCodeDF = DBUtil.loadDFFromTable("application.province_info", spark).select("code", "province")
    provinceCodeDF.createOrReplaceTempView("province")
    
//    val df = invDF.join(compProvinceDF).groupBy("src_id", "code").agg("src_id" -> "count").withColumnRenamed("count(src_id)", "inv_num")
//    val result = df.join(provinceCodeDF, "code")
    //company_id, province_code, inv_num
    val df = spark.sql("select i.src_id as company_id,p.province as province_code,count(i.src_id) as inv_num from invest i inner join comp_province p on i.dst_id = p.company_id group by src_id, province")
    val result = df.join(provinceCodeDF, $"province_code" === $"code").select("company_id", "province_code", "inv_num", "province")
    
    
    DBUtil.saveDFToDB(result, table)
  }
}