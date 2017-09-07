package com.proud.dc

import com.proud.dc.util.ImportUtil
import org.apache.spark.sql.SparkSession

/**
nohup spark-submit --class com.proud.dc.Save --master local[*] --jars /home/data_center/dependency/mysql-connector-java.jar --driver-momory 25G /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar chongqing zhejiang sichuan guizhou neimenggu xianxi jiangxi guangdong jiangsu &
spark-submit --class com.proud.dc.Save --master local[*] --driver-memory 25g --jars /home/data_center/dependency/mysql-connector-java.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar jiangsu zhejiang sichuan guizhou neimenggu xianxi jiangxi guangdong 
 */

object Save {
  val spark = SparkSession.builder().appName("DataImport").config("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse").enableHiveSupport().getOrCreate()
  val sc = spark.sparkContext
  def main(args: Array[String]): Unit = {
    if(args == null)
      System.exit(-1)
    
    for(province <- args){
    	val offset = getIdOffset(province)
    	if(province.equals("jiangsu")){
      	val reportOffset = 171960800L
        ImportUtil.saveToLocal(sc, province+".txt", province, offset, reportOffset)
    	}else{
    	  val reportOffset = offset
        ImportUtil.saveToLocal(sc, province+".txt", province, offset, reportOffset)
    	}
    }
  }
  
    //江苏原有最大ID：232422475
  def getIdOffset(province:String):Long = {
    val map = Map("chongqing" -> 470000000L, "zhejiang" -> 480000000L, "sichuan" -> 490000000L, "guizhou" -> 500000000L, "neimenggu" -> 510000000L, "xianxi" -> 520000000L,
        "jiangxi" -> 530000000L, "guangdong" -> 540000000L, "jiangsu" -> 232422500L, "jiangsu_report" -> 171960800L)
    map.get(province).getOrElse(1)
  }
}