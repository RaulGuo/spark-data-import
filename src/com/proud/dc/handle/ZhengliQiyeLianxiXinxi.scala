package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import com.proud.ark.config.DBName
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.KeyValueGroupedDataset
import com.proud.ark.config.GlobalVariables

/**
整理企业的联系人信息，包括email、postcode、telephone和website。
spark-submit --master local[*] --class com.proud.dc.handle.ZhengliQiyeLianxiXinxi --driver-memory 20G --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar shandong hebei
spark-shell --driver-memory 20G --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
spark-submit --class com.proud.dc.handle.ZhengliQiyeLianxiXinxi --master spark://bigdata01:7077 --executor-memory 10g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */

object ZhengliQiyeLianxiXinxi {
  val spark = SparkSession.builder().appName("QiyeTouziDiquFenbu").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
  import spark.implicits._
  import util.control.Breaks._
  
  case class EmailETC(company_id:Long, email:String, postcode:String, telephone:String)
  case class Website(company_id:Long, website:String)
  case class Contact(company_id:Long, email:String, postcode:String, telephone:String, website:String)
  
  def main(args: Array[String]): Unit = {
    val resultTable = s"${DBName.test}.company_contact_info"
    DBUtil.truncate(resultTable)
    val provinces = if(args == null || args.length == 0) GlobalVariables.provinces else args
    
    provinces.foreach { province => {
      val df = DBUtil.loadDFFromTable("dc_import."+province+"_company_report_base", spark).select("company_id", "email", "telephone", "postcode", "year").filter(x => {
        (x.getAs[String]("email") == null || x.getAs[String]("email").length() < 255) && (x.getAs[String]("telephone") == null ||  x.getAs[String]("telephone").length() < 255) && (x.getAs[String]("postcode") == null ||  x.getAs[String]("postcode").length() < 255) 
      }).groupByKey[Long]{ row:Row => row.getAs[Long]("company_id") }
      //获取email等保存于报表中的信息
      val reportDF = DBUtil.loadDFFromTable("dc_import."+province+"_company_report_base", spark).select("company_id", "id", "year").withColumnRenamed("id", "report_id")
      val websiteDF = DBUtil.loadDFFromTable("dc_import."+province+"_company_report_website", spark).select("report_id", "url").filter(x => {
        x.getAs[String]("url") == null || x.getAs[String]("url").length() < 255
      })
      val result = calcContactInfo(df, province, reportDF, websiteDF).filter(x => {
        if(x.email == null && x.postcode == null && x.telephone == null && x.website == null){
          false
        }else{
          true
        }
      })
      
      DBUtil.saveDFToDB(result, resultTable)
      
    }}
    
    def calcContactInfo(df:KeyValueGroupedDataset[Long, Row], province:String, reportDF:Dataset[Row], websiteDF:Dataset[Row]) = {
      val emailEtcDF = df.mapGroups[EmailETC]{(companyId:Long, it:Iterator[Row]) => {
        var email:String = null
        var postcode:String = null
        var telephone:String = null
        var year:Int = 0
        it.foreach { r => {
        	breakable{
          try{
              val currYear = r.getAs[String]("year").trim().toInt
              //如果已有年份小于当前年份，则如果当前年份中有数据，则需要更新
              if(currYear > year){
                var currEmail:String = r.getAs[String]("email")
                var currPostcode:String = r.getAs[String]("postcode")
                var currTelephone:String = r.getAs[String]("telephone")
                if(currEmail != null && currEmail.length() != 0){
                  email = currEmail
                }
                if(currPostcode != null && currPostcode.length() != 0){
                  postcode = currPostcode
                }
                if(currTelephone != null && currTelephone.length() != 0){
                  telephone = currTelephone
                }
                
              }else{
                //如果已有年份小于当前年，则只有字段为空时才去更新
                if(email == null || email.length() == 0){
                  email = r.getAs[String]("email")
                }
                if(postcode == null || postcode.length() == 0){
                  postcode = r.getAs[String]("postcode")
                }
                if(telephone == null || telephone.length() == 0){
                  telephone = r.getAs[String]("telephone")
                }
              }
              
              year = currYear
          }catch{
            case e:NumberFormatException => break
          }
        } }}
        
        new EmailETC(companyId, email, postcode, telephone)
      }}
      
      val compWebsiteDF = reportDF.join(websiteDF, "report_id").select("company_id", "year", "url").groupByKey[Long] { x:Row => x.getAs[Long]("company_id") }
      val websiteDS = compWebsiteDF.mapGroups[Website]((companyId:Long, it:Iterator[Row]) => {
    	  var year:Int = 0
    		var url:String = null
        it.foreach { x => {
          val currYear = x.getAs[String]("year").trim().toInt
          if(currYear > year){
            var currUrl = x.getAs[String]("url")
            if(currUrl != null && currUrl.length() > 0)
              url = currUrl
          }else{
            if(url == null || url.length == 0)
              url = x.getAs[String]("url")
          }
          year = currYear
        }}
        new Website(companyId, url)
      })
      
      val result =  emailEtcDF.joinWith[Website](websiteDS, emailEtcDF.col("company_id").equalTo(websiteDS.col("company_id")), "outer").map(x => {
        val e = x._1
        val w = x._2
        if(e != null)
          if(w != null)
            Contact(e.company_id, e.email, e.postcode, e.telephone, w.website)
          else
            Contact(e.company_id, e.email, e.postcode, e.telephone, null)
        else
          Contact(w.company_id, null, null, null, w.website)
      })
      result
    }
    
    
  }
}