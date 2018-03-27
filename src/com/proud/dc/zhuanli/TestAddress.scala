package com.proud.dc.zhuanli

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.SaveMode
import org.jsoup.Jsoup

object TestAddress {
  case class Address(id:Long, address:String)
  case class Gis(latitude:Double, longitude:Double, city:String, district:String, text:String, address:String)
  
  case class policy(id:Integer, content:String, title:String, time:String, yuan:String)
  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanliZhuanliToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse)
	  .master(ConfigUtil.master).getOrCreate()
	  
	  import spark.implicits._
	  
	  DBUtil.loadDFFromICTable("entity.policy", 16, spark).select("id", "content", "title", "time", "yuan").map(x => {
	    policy(x.getAs[Integer]("id"), Jsoup.parse(x.getAs[String]("content")).text(), x.getAs[String]("title"), x.getAs[String]("time"), x.getAs[String]("yuan"))
	  })
	  
	  
	  
	  val shanghaiAddress = DBUtil.loadDFFromTable("dc_import.shanghai_company", 32, spark).select("id", "address", "type").filter(x => x.getAs[String]("type") == null || !x.getAs[String]("type").startsWith("个体")).map(x => {
	    val address = x.getAs[String]("address");
	    val trim = if(address == null || address.trim.isEmpty()) null else address.trim()
	    Address(x.getAs[Long]("id"), trim)
	  }).persist();
     
    val addressInfo = DBUtil.loadDFFromTable("test.aa_shanghai_tmp_address", 32, spark).select("latitude", "longitude", "city", "district", "text", "address").persist();
    
    shanghaiAddress.createOrReplaceTempView("address")
    addressInfo.createOrReplaceTempView("gis")
    
    val joinResult = spark.sql("select latitude, longitude, city, district, text, a.address, id from address a inner join gis g on a.address = g.address").dropDuplicates("id")
    
    val left = spark.sql("select a.address, id from address a left join gis g on a.address = g.address where g.address is null")
    
    DBUtil.saveDFToDB(joinResult, "tmp.shanghai_address", SaveMode.Overwrite)
    DBUtil.saveDFToDB(left, "tmp.shanghai_address_to_append", SaveMode.Overwrite)
    
  }
  
}