package com.proud.dc.zhuanli

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.db.Prod153IntraDBUtil
/**
 * nohup spark-submit --executor-memory 10g --master spark://bigdata01:7077 --class com.proud.dc.zhuanli.ZhuanliTupianChuli --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
 */
object ZhuanliTupianChuli {
  
  case class PatentPic(shouquan_gonggaohao:String, image:String)
  
  val map = Map("CN8" -> "201001", "CN9" -> "201001", "CN100" -> "201002", "CN101" -> "201003", "CN102" -> "201004", "CN103" -> "201005", "CN104" -> "201006", "CN105" -> "201007", "CN106" -> "201008", "CN107" -> "201009", "CN108" -> "201010", "CN109" -> "201011", "CN11" -> "2010012", "CN12" -> "201101", "CN13" -> "201102", "CN14" -> "201103", "CN15" -> "201104", "CN16" -> "201105", "CN17" -> "201106", "CN18" -> "201107", "CN19" -> "201108", "CN200" -> "201109", "CN201" -> "201110", "CN202" -> "201111", "CN203" -> "201112", "CN204" -> "201201", "CN205" -> "201202", "CN206" -> "201203", "CN207" -> "201304", "CN208" -> "201305", "CN209" -> "201306", "CN21" -> "201307", "CN22" -> "201308", "CN23" -> "201309", "CN24" -> "201310", "CN25" -> "201311", "CN26" -> "201312", "CN27" -> "201401", "CN28" -> "201402", "CN29" -> "201403", "CN300" -> "201404", "CN301" -> "201405", "CN302" -> "201406", "CN303" -> "201407", "CN304" -> "201408", "CN305" -> "201409", "CN306" -> "201410", "CN307" -> "201411", "CN308" -> "201412", "CN309" -> "201501", "CN31" -> "201502", "CN32" -> "201503", "CN33" -> "201504", "CN34" -> "201505", "CN35" -> "201506", "CN36" -> "201507")
  
  val keys = map.keySet
  
  val prefix = "images/patent/"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanliZhuanliToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse)
	  .master(ConfigUtil.master).getOrCreate()
	  
	  
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  
	  val zhuanliDF = Prod153IntraDBUtil.loadDFFromTable("patent.patent_pic", 1000, spark).select("shouquan_gonggaohao", "pic").map { x => {
	    val gonggaohao = x.getAs[String]("shouquan_gonggaohao");
	    val pic = x.getAs[String]("pic")
	    val newPic = transPic(pic)
	    PatentPic(gonggaohao, newPic)
	  } }
	  
    
	  Prod153IntraDBUtil.insertOnUpdateDFToDB(zhuanliDF, "patent.zhuanli_xinxi_tmp");
    
  }
  
  def transPic(pic:String):String = {
    if(pic == null || pic.trim.isEmpty){
      return null;
    }
    
    var dir:String = null
    val filename = pic.substring(prefix.length()-1, pic.length())
    
    var suffix1 = pic.substring(prefix.length(), prefix.length()+3)
    if(keys.contains(suffix1))
      dir = map(suffix1)
    else{
      suffix1 = pic.substring(prefix.length(), prefix.length()+4)
      if(keys.contains(suffix1))
          dir = map(suffix1)
      else{
        suffix1 = pic.substring(prefix.length(), prefix.length()+5)
        if(keys.contains(suffix1))
          dir = map(suffix1)
      }
    }
    
    if(dir != null)
      return prefix+dir+filename
    
    return null
  }
}