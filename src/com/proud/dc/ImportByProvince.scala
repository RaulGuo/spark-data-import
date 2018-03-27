package com.proud.dc

import com.proud.dc.util.ImportUtil
import com.proud.dc.util.ImportReportThreads
import com.proud.dc.util.ImportCompanyThreads
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil

/**
nohup spark-submit --class com.proud.dc.ImportByProvince --master local[*] --jars /home/data_center/dependency/mysql-connector-java.jar --driver-momory 25G /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar chongqing zhejiang sichuan guizhou neimenggu xianxi jiangxi guangdong jiangsu &
spark-submit --class com.proud.dc.ImportByProvince --master local[*] --driver-memory 25g --jars /home/data_center/dependency/mysql-connector-java.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar chongqing zhejiang sichuan guizhou neimenggu xianxi jiangxi guangdong
spark-submit --class com.proud.dc.ImportByProvince --master spark://bigdata01:7077 --executor-memory 12g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar chongqing zhejiang sichuan guizhou neimenggu xianxi jiangxi guangdong

 */

object ImportByProvince {
  val spark = SparkSession.builder().appName("DataImport").master("local[*]").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).enableHiveSupport().getOrCreate()
  
  val sc = spark.sparkContext
  def main(args: Array[String]): Unit = {
    println("-------------------------"+ImportCompanyThreads.schema+"--------------")  
    if(args == null)
      System.exit(-1)
    
    for(province <- args){
      
      val (df, reportDF) = ImportUtil.addIdToData(sc, province+".txt", getIdOffset(province), getIdOffset(province))
      ImportCompanyThreads.importCompanyRelatedWithDF(spark, province, getProvince(province), df)
//      ImportReportThreads.importReportRelatedWithDF(spark, province, getProvince(province), reportDF)
    }
  }
  
  val idOffset = Map("anhui" -> 1L, "beijing" -> 20000001L, "fujian" -> 40000001L, 
    "gansu" -> 60000001L, "guangxi" -> 80000001L, "hainan" -> 100000001L, "hebei" -> 120000001L, "heilongjiang" -> 140000001L, "henan" -> 160000001L, "hubei" -> 180000001L, "hunan" -> 200000001L, 
    "jilin" -> 240000001L, "liaoning" -> 260000001L, "ningxia" -> 280000001L, "qinghai" -> 300000001L, "shandong" -> 320000001L, 
    "shanxi" -> 360000001L, "tianjin" -> 380000001L, "xinjiang" -> 400000001L, "xizang" -> 420000001L, "yunnan" -> 440000001L, "zongju" -> 460000001L,
    "chongqing" -> 470000001L, "guangdong" -> 490000000L, "guizhou" -> 510000000L, "jiangxi" -> 530000000L, "neimenggu" -> 550000000L, "sichuan" -> 570000000L, "xianxi" -> 590000000L, "zhejiang" -> 610000000L, "jiangsu" -> 630000000L
    , "shanghai" -> 650000000L)
  
  def getIdOffset(province:String):Long = {
    idOffset.get(province).getOrElse(1L)
  }
  
  val provinceMap = Map("anhui" -> 1, "beijing" -> 2, "fujian" -> 3, 
    "gansu" -> 4, "guangxi" -> 5, "hainan" -> 6, "hebei" -> 7, "heilongjiang" -> 8, "henan" -> 9, "hubei" -> 10, "hunan" -> 11, "jiangsu" -> 12,
    "jilin" -> 13, "liaoning" -> 14, "ningxia" -> 15, "qinghai" -> 16, "shandong" -> 17, "shanghai" -> 18,
    "shanxi" -> 19, "tianjin" -> 20, "xinjiang" -> 21, "xizang" -> 22, "yunnan" -> 23, "zongju" -> 24,
    "guangdong" -> 25, "chongqing" -> 26, "zhejiang" -> 27, "sichuan" -> 28, "guizhou" -> 29, "neimenggu" -> 30, "xianxi" -> 31, "jiangxi" -> 32)
  
  
  def getProvince(province:String):Int = {
    provinceMap.get(province).getOrElse(-1)
  }
  
}