package com.proud.dc.handle

import com.proud.ark.db.DBUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.config.DBName
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.proud.ark.data.HDFSUtil
import org.apache.spark.sql.SaveMode

/**
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
nohup spark-submit --master spark://bigdata01:7077 --executor-memory 10G --class com.proud.dc.handle.ZhengliQiyeGudongBySQL --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &

 */

object ZhengliQiyeGudongBySQL {
  val spark = SparkSession.builder().appName("QiyeGudong").master(ConfigUtil.master).config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._
  val table = s"${DBName.test}.gudong_xinxi"
  
  def main(args: Array[String]): Unit = {
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).persist()
    
    //id, name, md5
    companyDF.createOrReplaceTempView("company_basic")
    
    if(args == null || args.length == 0){
      val stockHolderDF = DBUtil.loadDFFromTable("dc_import.company_stock_holder", 16, spark).select("company_id", "name").filter(x => x.getAs[String]("name") != null && x.getAs[String]("name").length() > 0)
      zhengliGudong(stockHolderDF, SaveMode.Overwrite)
    } else {
      for(province <- args){
        val stockHolderDF = DBUtil.loadDFFromTable("dc_import."+province+"_company_stock_holder", spark).select("company_id", "name").filter(x => x.getAs[String]("name") != null && x.getAs[String]("name").length() > 0)
        zhengliGudong(stockHolderDF)
      }
    }
    
  }
  
  def zhengliGudong(stockHolderDF:Dataset[Row], mode:SaveMode = SaveMode.Append){
      //company_id, name
      stockHolderDF.createOrReplaceTempView("stock_holder")
      
      val sql = "select s.company_id, b1.name as company_name, b1.md5 as company_md5, b2.id as gudong_id, b2.md5 as gudong_md5, s.name as gudong_name from company_basic b1 inner join stock_holder s on b1.id = s.company_id left join company_basic b2 on s.name = b2.name"
      
      val df = spark.sql(sql).dropDuplicates("company_id", "gudong_id").dropDuplicates("company_name", "gudong_name")
      
      HDFSUtil.saveDSToHDFS(GlobalVariables.hdfsGudongXinxi, df, mode)
      DBUtil.saveDFToDB(df, table)
  }
}