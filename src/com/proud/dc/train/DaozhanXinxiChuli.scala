package com.proud.dc.train

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import java.util.ArrayList

object DaozhanXinxiChuli {
  case class Train(checi:String, station_name:String, daoda_shijian:String, fache_shijian: String, licheng:String)
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("CompanyRelatedCount").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
    import spark.implicits._
    
    val ds = DBUtil.loadDFFromTable("test.train_daozhan", 16, spark).select("checi", "station_name", "daozhan_shijian", "fache_shijian", "licheng").as[Train].groupByKey { x => x.checi }
    
    ds.mapGroups((checi:String, it:Iterator[Train]) => {
      var set = it.toSet
      var list = new ArrayList[Train];
      val min = set.minBy(getTrainObjScore)
      
      1
    })
  }
  
  def getTrainObjScore(t:Train):Int = {
    t.licheng.replace(":", "").toInt
  }
  
//  def compareTrain(t1:Train, t2:Train):Boolean = {
//    if(t1.licheng.equals("00:00")){
//      return false;
//    }else
//      return t1.licheng < t2.licheng;
//  }
}