package com.proud.dc

import com.proud.dc.util.ImportUtil
import com.proud.dc.util.ImportReportThreads
import com.proud.dc.util.ImportCompanyThreads
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.data.HDFSUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 * 导入的数据中包含两部分：一部分为原始数据，一部分为补充数据。
 * 将补充数据添加-append后缀名放到HDFS中，从hdfs中读取，
 * 根据name做去重。
 *
 */

object ImportWithAppendByProvince {
  val spark = SparkSession.builder().appName("DataImport").master(ConfigUtil.master).config("spark.sql.warehouse.dir", ConfigUtil.warehouse).enableHiveSupport().getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  case class Name(name:String)
  
  def main(args: Array[String]): Unit = {
    println("-------------------------"+ImportCompanyThreads.schema+"--------------")  
    if(args == null)
      System.exit(-1)
    
    for(province <- args){
      val origDF = spark.read.json(HDFSUtil.hdfsUrl+"/home/data_center/source/"+province+".txt")
      val appendDF = spark.read.json(HDFSUtil.hdfsUrl+"/home/data_center/source/"+province+"-append.txt")
      
      val names = DBUtil.loadDFFromTable("dc_import_append.sic_name", spark).map{ x =>
        x.getAs[String]("name")
      }.collect().toSet
      val broad = sc.broadcast(names)
      
      val filterDF = origDF.filter(x => {
        val name = x.getAs[GenericRowWithSchema]("bus").getAs[GenericRowWithSchema]("base").getAs[String]("name")
        !"".equals(name) && !(broad.value.contains(name))
      })
      
      
      
      
      val count = filterDF.count
      origDF.printSchema
      appendDF.printSchema
      
      val (df, reportDF) = ImportUtil.addIdToDF(sc, filterDF, ImportByProvince.getIdOffset(province), ImportByProvince.getIdOffset(province))
      ImportCompanyThreads.importCompanyRelatedWithDF(spark, province, ImportByProvince.getProvince(province), df)
      ImportReportThreads.importReportRelatedWithDF(spark, province, ImportByProvince.getProvince(province), reportDF)
      
      val (df2, reportDF2) = ImportUtil.addIdToDF(sc, appendDF, ImportByProvince.getIdOffset(province)+count+100000, ImportByProvince.getIdOffset(province)+count+2903383)
      ImportCompanyThreads.importCompanyRelatedWithDF(spark, province, ImportByProvince.getProvince(province), df2)
      ImportReportThreads.importReportRelatedWithDF(spark, province, ImportByProvince.getProvince(province), reportDF2)
    }
  }
  
  
}