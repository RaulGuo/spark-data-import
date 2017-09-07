package com.proud.dc.test

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.dc.util.Util
import org.apache.spark.sql.SaveMode
import com.proud.ark.config.DBName

/**
nohup spark-submit --master local[*] --driver-memory 10G --class com.proud.dc.test.CalcAvgSalary --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
 */

object CalcAvgSalary {
  case class AvgSalary(content:String, salary:Double)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GuanlianZhaopinQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    
    val job51 = DBUtil.loadCrawlerData("51job_jobs_info", spark).select("salary").map(x => new AvgSalary(x.getAs("salary"), Util.calc51jobAvgSalary(x.getAs("salary")))).filter(x => x.salary == -1)
    DBUtil.saveDFToDB(job51, s"${DBName.jobs}.51job_salary", SaveMode.Overwrite)
    
    val zhilian = DBUtil.loadCrawlerData("zhilian_info", spark).select("salary").map(x => new AvgSalary(x.getAs("salary"), Util.calcZhilianSalary(x.getAs("salary")))).filter(x => x.salary == -1)
    DBUtil.saveDFToDB(job51, s"${DBName.jobs}.zhilian_salary", SaveMode.Overwrite)
  }
}