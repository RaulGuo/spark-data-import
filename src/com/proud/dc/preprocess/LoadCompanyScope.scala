package com.proud.dc.preprocess

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.db.ProdIntranetDBUtil
import com.proud.ark.data.HDFSUtil

/**
 * spark-submit --driver-memory 10g --class com.proud.dc.preprocess.LoadCompanyScope --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */

object LoadCompanyScope {
  def main(args: Array[String]): Unit = {
		  val spark = SparkSession.builder().appName("CheckAbnormalColumns").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
		  val df = ProdIntranetDBUtil.loadDFFromTable("dc_import.company", 32, spark).select("id", "type", "scope").filter(x => {
	      val t = x.getAs[String]("type")
	      val scope = x.getAs[String]("scope")
	      ((t == null || !t.startsWith("个体")) && scope != null && !scope.trim().isEmpty())
	    })
	    
	    HDFSUtil.saveDSToHDFS("/home/data_center/CompanyScope", df)
  }
}