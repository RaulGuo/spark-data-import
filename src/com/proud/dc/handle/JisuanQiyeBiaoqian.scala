package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
import com.proud.ark.config.DBName
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.proud.ark.data.HDFSUtil

/**
 * 计算企业的标签
 * 目前需要处理的标签类型保存在dc_import.company_tag_name表中，包括：
 * 
spark-submit --class com.proud.dc.handle.JisuanQiyeBiaoqian --master spark://bigdata01:7077 --executor-memory 10g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
高新技术企业 tag.gaoxinjishu
新三板企业 tag.xinsanban
新四板企业 tag.xinsiban
A级纳税人 tag.aji_nashui
主板企业 tag.zhuban_qiye
 * 
 */
object JisuanQiyeBiaoqian {
  val spark = SparkSession.builder().appName("QiyeBiaoqian").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master(ConfigUtil.master).getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._
  
  case class Tag(company_id:Long, tag_id:Int, tag_name:String)
  
  val gaoxinjishuDF = DBUtil.loadDFFromTable("tag.gaoxinjishu", spark).select("name").distinct()
  val xinsanbanDF = DBUtil.loadDFFromTable("tag.xinsanban", spark).select("name").distinct()
  val xinsibanDF = DBUtil.loadDFFromTable("tag.xinsiban", spark).select("name").distinct()
  val ajinashuiDF = DBUtil.loadDFFromTable("tag.aji_nashui", spark).select("name").distinct()
  val zhubanDF = DBUtil.loadDFFromTable("tag.zhuban_qiye", spark).select("name").distinct()

  def main(args: Array[String]): Unit = {
    if(args == null || args.length == 0){
      DBUtil.truncate(s"${DBName.dc_import}.company_tag")
      val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name")
      zhengliBiaoqian(companyDF)
    }else{
      for(province <- args){
        val companyDF = DBUtil.loadDFFromTable("dc_import."+province+"_company", spark).select("id", "name").persist(StorageLevel.MEMORY_AND_DISK_SER)
        zhengliBiaoqian(companyDF)
      }
    }
  }
  
  def zhengliBiaoqian(companyDF:Dataset[Row]){
    val gaoxinTag = gaoxinjishuDF.join(companyDF, "name").map { x => {
        val companyId = x.getAs[Long]("id")
        new Tag(companyId, 1, "高新技术企业")
      } }
      DBUtil.saveDFToDB(gaoxinTag, s"${DBName.dc_import}.company_tag")
      
      val xinsanbanTag = xinsanbanDF.join(companyDF, "name").map { x => {
        val companyId = x.getAs[Long]("id")
        new Tag(companyId, 2, "新三板企业")
      } }
      DBUtil.saveDFToDB(xinsanbanTag, s"${DBName.dc_import}.company_tag")
      
      val xinsibanTag = xinsibanDF.join(companyDF, "name").map { x => {
        val companyId = x.getAs[Long]("id")
        new Tag(companyId, 3, "新四板企业")
      } }
      DBUtil.saveDFToDB(xinsibanTag, s"${DBName.dc_import}.company_tag")
      
      val ajinashuiTag = ajinashuiDF.join(companyDF, "name").map { x => {
        val companyId = x.getAs[Long]("id")
        new Tag(companyId, 4, "A级纳税人")
      } }
      DBUtil.saveDFToDB(ajinashuiTag, s"${DBName.dc_import}.company_tag")
      
      val zhubanbTag = zhubanDF.join(companyDF, "name").map { x => {
        val companyId = x.getAs[Long]("id")
        new Tag(companyId, 5, "主板企业")
      } }
      DBUtil.saveDFToDB(zhubanbTag, s"${DBName.dc_import}.company_tag")
  }
  
}