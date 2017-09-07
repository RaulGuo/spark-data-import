package com.proud.dc.statistics

import org.apache.spark.sql.SparkSession
import java.util.Date
import java.util.Calendar
import org.apache.spark.sql.SaveMode
import com.google.gson.Gson
import com.proud.dc.statistics.model.ZhaopinQushi
import com.proud.dc.util.Util
import org.apache.spark.util.SizeEstimator
import org.apache.spark.storage.StorageLevel
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.config.DBName

/**
spark-submit --master spark://bigdata01:7077 --executor-memory 9G --class com.proud.dc.statistics.JisuanZhaopinQushi --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */

object JisuanZhaopinQushi {
  
  case class Tongji(company_id:Long, month:Int, year:Int)
  
  def main(args: Array[String]): Unit = {
	  val spark = SparkSession.builder().appName("JisuanZhaopinQushi").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
		import spark.implicits._
	  val ds = DBUtil.loadDFFromTable("jobs.company_employment", spark).select("company_id", "publish_at").filter(x => x.getAs[Date]("publish_at") != null).map { row => {
	    val companyId = row.getAs[Long]("company_id")
	    val publishAt = row.getAs[Date]("publish_at")
	    var cal = Calendar.getInstance
	    cal.setTime(publishAt);
	    val year = cal.get(Calendar.YEAR)
	    val month = cal.get(Calendar.MONTH)
	    new Tongji(companyId, month+1, year)
	  } }
	  
	  val result = ds.groupBy("company_id", "year", "month").count().withColumnRenamed("count", "number")
    
//	  val tongjixinxiDF = rdd.groupBy(row => (row.company_id, row.year, row.month)).map(row => {
//	    val first = row._1
//	    val second = row._2
//	    (first._1, first._2, second.count(row => true), first._3)
//	  }).toDF("company_id", "month", "number", "year")
	  
	  DBUtil.saveDFToDB(result, s"${DBName.jobs}.qiye_zhaopin_tongji")
  }
  
}


  