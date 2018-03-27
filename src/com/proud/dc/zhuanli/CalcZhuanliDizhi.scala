package com.proud.dc.zhuanli

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import java.util.HashMap
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.BufferedReader
import com.google.gson.Gson
import com.proud.ark.dizhi.ZhongguoDizhiUtil

/**
 * nohup spark-submit --class com.proud.dc.zhuanli.CalcZhuanliDizhi --executor-memory 10g --master spark://bigdata01:7077 --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar  /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
 */

object CalcZhuanliDizhi {
  
  case class ZhuanliDizhi(id:Long, dizhi:String, old_city:String, old_province:String, new_city:String, new_province:String)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanliZhuanliToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse)
	  .master(ConfigUtil.master).getOrCreate()
	  
	  import spark.implicits._
	  val sc = spark.sparkContext
	  
	  val df = DBUtil.loadDFFromTable("patent.zhuanli_xinxi", 64, spark).select("id", "dizhi", "city", "province").map(x => {
	    val arr = ZhongguoDizhiUtil.getProvinceAndCity(x.getAs[String]("dizhi"))
	    ZhuanliDizhi(x.getAs[Long]("id"), x.getAs[String]("dizhi"), x.getAs[String]("city"), x.getAs[String]("province"), arr(1), arr(0))
	  })
	  
	  val result = df.filter(x => x.old_city != x.new_city || x.old_province != x.new_province)
	  
	  DBUtil.saveDFToDB(result, "patent.zhuanli_dizhi", SaveMode.Overwrite)
	  
	  println("------------------------totol number of result: "+result.count()+"----------------")

//   val address = "英属维尔京群岛托朵拉城市"
//	 val v1 = getProvinceAndCity(address)
//	 
//	 println(v1(0)+" -> "+v1(1))
	  
  }
  
  
  //返回结果为省，市

}