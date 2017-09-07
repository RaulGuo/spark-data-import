package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
/**
spark-submit --class com.proud.dc.handle.CheckAbnormalColumns --master spark://bigdata01:7077 --executor-memory 13g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */
object CheckAbnormalColumns {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CheckAbnormalColumns").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
	  val sc = spark.sparkContext
	  import spark.implicits._
	  val df = DBUtil.loadDFFromTable("dc_import.company", spark).select("id", "leg_rep", "start_at")
	  val result = df.filter(x => {
	    val l1 = if(x.getAs[String]("leg_rep") == null) 0 else x.getAs[String]("leg_rep").length
	    val l2 = if(x.getAs[String]("start_at") == null) 0 else x.getAs[String]("start_at").length
	    l1 > 25 || l2 > 25
	  })
	  DBUtil.saveDFToDB(result, "test.leg_reg_start_at")
  }
}