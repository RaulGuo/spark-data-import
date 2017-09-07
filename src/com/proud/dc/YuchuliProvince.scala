package com.proud.dc

import org.apache.spark.sql.SparkSession
import com.proud.dc.util.ImportUtil

object YuchuliProvince {
  val spark = SparkSession.builder().appName("DataImport").config("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse").enableHiveSupport().getOrCreate()
	val sc = spark.sparkContext
			
  def main(args: Array[String]): Unit = {
    if(args == null)
      System.exit(-1)
    
    for(province <- args){
      ImportUtil.saveToLocal(sc, province+".txt", province)
    }
  }
}