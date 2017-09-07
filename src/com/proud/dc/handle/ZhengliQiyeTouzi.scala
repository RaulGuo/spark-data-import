package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import com.proud.ark.config.DBName
import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.GlobalVariables

/**
 * 整理企业的投资信息（包含股东信息中整理出来的投资信息，所以要先运行ZhengliQiyeGudongBySQL）
 * 要注意将test的库换成其他的相应的库。
 * 
nohup spark-submit --master spark://bigdata01:7077 --executor-memory 10g --class com.proud.dc.handle.ZhengliQiyeTouzi --jars /root/data/dependency/mysql-connector-java.jar,/root/data/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/datacenter-import-0.0.1-SNAPSHOT.jar &
读取的数据来自company_report_invest_enterprise，company_report_base和company。
注意事项：最后写入数据库的模式是Append，最好将表清空
 */

object ZhengliQiyeTouzi {
  
  def main(args: Array[String]): Unit = {
	  val spark = SparkSession.builder().appName("QiyeGudong").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
	  val sc = spark.sparkContext
	  import spark.implicits._
    val resultTable = s"${DBName.dc_import}.company_investment_enterprise"
    DBUtil.truncate(resultTable)
    
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark)
    //id, name, md5
    companyDF.createOrReplaceTempView("company")
    
    val reportDF = DBUtil.loadDFFromTable("dc_import.company_report_base", spark).select("id", "company_id")
    //id, company_id
    reportDF.createOrReplaceTempView("report")
    
    val reportInvEntDF = DBUtil.loadDFFromTable("dc_import.company_report_invest_enterprise", spark).select("report_id", "reg_no", "name").filter { x => x.getAs[String]("name") == null || (x.getAs[String]("name")).trim().length() == 0  }
    //report_id, reg_no, name
    reportInvEntDF.createOrReplaceTempView("investment")
    
    //1.整理企业投资了哪些企业
    val sql = "select c1.id as src_id, c1.name as src_name, c1.md5 as src_md5, i.reg_no, c2.id as dst_id, c2.md5 as dst_md5, i.name as dst_name from company c1 inner join report r on c1.id = r.company_id inner join investment i on r.id = i.report_id left join company c2 on i.name = c2.name"
    //重新select一遍是为了column统一顺序，用于union
    val df = spark.sql(sql).select("dst_md5", "dst_id", "dst_name", "reg_no", "src_id", "src_name", "src_md5")
    df.printSchema()
//    DBUtil.saveDFToDB(df, resultTable)
    
    //将股东信息加载进来，与企业信息进行合并
//    val gudongDF = DBUtil.loadDFFromTable("test.gudong_xinxi", spark).filter(x => x.getAs[Long]("gudong_id") != null).withColumnRenamed("gudong_id", "src_id").withColumnRenamed("gudong_name", "src_name").withColumnRenamed("gudong_md5", "src_md5").withColumnRenamed("company_id", "dst_id").withColumnRenamed("company_md5", "dst_md5").withColumnRenamed("company_name", "dst_name").withColumn("reg_no", lit(null))
    val gudongDF = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsGudongXinxi, spark).filter(x => x.getAs[Long]("gudong_id") != null).withColumnRenamed("gudong_id", "src_id").withColumnRenamed("gudong_name", "src_name").withColumnRenamed("gudong_md5", "src_md5").withColumnRenamed("company_id", "dst_id").withColumnRenamed("company_md5", "dst_md5").withColumnRenamed("company_name", "dst_name").withColumn("reg_no", lit(null)).select("dst_md5", "dst_id", "dst_name", "reg_no", "src_id", "src_name", "src_md5")
    val unionDF = df.union(gudongDF)
//    println("-----------------the result count of union: "+unionDF.count)
    val result = unionDF.dropDuplicates("src_name", "dst_name")
//    println("-----------------the result count of unique: "+result.count)
    
    DBUtil.saveDFToDB(result, resultTable)
    
  }
}