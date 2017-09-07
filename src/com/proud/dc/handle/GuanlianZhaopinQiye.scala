package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.data.HDFSUtil

/**
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
nohup spark-submit --master local[*] --driver-memory 20G --class com.proud.dc.handle.GuanlianZhaopinQiye --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &

 */

object GuanlianZhaopinQiye {
  val spark = SparkSession.builder().appName("GuanlianZhaopinQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._
  
  def main(args: Array[String]): Unit = {
    //id, company_name, company_id, 其中company_id只用于进行过滤
    val employDF = DBUtil.loadDFFromTable("jobs.company_employment", spark).select("id", "company_name", "company_id").filter(x => x.getAs[Long]("company_id") == null).select("id", "company_name")
    
    val company = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "company_id")
    
    //获取招聘ID对应的企业ID，并过滤掉为空的企业
    val result = employDF.join(company, $"company_name" === $"name").select("id", "company_id").filter(_.getAs[Long]("company_id") != null)
    
    DBUtil.saveDFToDB(result, "test.company_employment_medium")
    
  }
}