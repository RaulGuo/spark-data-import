package com.proud.dc.statistics

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.Row
import scala.math.BigDecimal
import util.control.Breaks._
import com.proud.ark.config.GlobalVariables
import com.proud.ark.data.HDFSUtil

/**
 * 计算企业的持股比例
 * 
 */

object JisuanChiguBili {
	case class ReportInfo(reportId:Long, companyId:Long, year:Int)
	case class ReportInvestInfo(reportId:Long, amount:Double, name:String)
	case class BiliStructure(company_id:Long, gudong_name:String, percent:Double, amount:Double, total:Double, erji_id:Long, erji_name:String, erji_percent:Double, erji_amount:Double, erji_total:Double,sanji_id:Long, sanji_name:String, sanji_percent:Double, sanji_amount:Double, sanji_total:Double)
	case class Attr(gudong_name:String, percent:Double, amount:Double, total:Double, gudong_id:Long) 

	case class Bili(company_id:Long, gudong_name:String, percent:Double, amount:Double, total:Double)
	case class BiliErji(company_id:Long, attrs:Array[Attr], children:Array[Bili])
	case class BiliSanji(comany_id:Long, attrs:Array[Attr], children:Array[BiliErji])
	
	val resultTable = "test.company_stock_percent"
	
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().master(ConfigUtil.master).appName("JisuanChiguBili").config("spark.sql.warehouse.dir", ConfigUtil.warehouse)
				.master(ConfigUtil.master).getOrCreate()
				
		import spark.implicits._
    DBUtil.truncate(resultTable)
    
    //company_id, company_name
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "gudong_id").withColumnRenamed("name", "company_name").persist()
    
    GlobalVariables.provinces.foreach { p => 
  	  val reportInfo = DBUtil.loadDFFromTable(s"dc_import.${p}_company_report_base", spark).select("id", "company_id", "year").map(x => {
  	    val year = x.getAs[String]("year")  
  	    if(year != null && year.matches("\\d+"))
  	      ReportInfo(x.getAs[Long]("id"), x.getAs[Long]("company_id"), year.toInt)
  	    else
  	      ReportInfo(x.getAs[Long]("id"), x.getAs[Long]("company_id"), -1)
  	  }).filter(_.year != -1)
  	  
  	  val investDF = DBUtil.loadDFFromTable(s"dc_import.${p}_company_report_stock_investment", spark).select("report_id", "act_amount", "name").filter(x => (x.getAs[String]("act_amount") != null && x.getAs[String]("act_amount").trim().length() != 0))
  	  
  	  val invInfo = investDF.map(x => {
  	    ReportInvestInfo(x.getAs[Long]("report_id"), trimNumFromLeft(x.getAs[String]("act_amount")), x.getAs[String]("name"))
  	  })
  	  //过滤出每个公司最新一年的报表，根据最新一年的报表来计算持股比例。
  	  val filterReport = reportInfo.groupByKey(r => r.companyId).reduceGroups{(r1:ReportInfo, r2:ReportInfo) => {
  	    if(r1.year > r2.year)
  	      r1
  	    else
  	      r2
  	  }}.map(x => x._2)
  	  
  	  //计算出企业的各个股东持股比例
  	  val biliDS = filterReport.join(invInfo, "reportId").select("companyId", "name", "amount").groupByKey(x => x.getAs[Long]("companyId")).flatMapGroups{(companyId:Long, it:Iterator[Row]) => {
  	    val sum = it.map { x => x.getAs[Double]("amount") }.reduce(_+_)
  	    //结果中保留两位小数
  	    val r = it.map { r => {
  	        Bili(companyId, r.getAs[String]("name"), (math rint r.getAs[Double]("amount")/sum*10000)/10000, r.getAs[Double]("amount"), sum)
  	      }
  	    }
  	    r
  	  }}
  	  
  	  val chigubiliXinxiDF = biliDS.join(companyDF, $"gudong_name" === $"company_name").select("company_id", "gudong_name", "percent", "amount", "total", "gudong_id").persist()
   	  chigubiliXinxiDF.createOrReplaceTempView("chigu_bili")

   	  val chigubiliGroupDF = chigubiliXinxiDF.groupByKey(x => x.getAs[Long]("company_id")).mapGroups{case(company_id, it) => {
   	    1
   	  }}
   	  
  	  
  	  val erjiStructure = spark.sql("select b1.company_id, b1.gudong_name, b1.percent, b1.amount, b1.total, b1.gudong_id as erji_id,b2.gudong_name as erji_name, b2.percent as erji_percent, b2.amount as erji_amount, b2.total as erji_total, b2.gudong_id as sanji_id"
  	      +" from chigu_bili b1 left join chigu_bili b2 on b1.gudong_id = b2.company_id")
  	  
  	  
  	  val structure = spark.sql("select b1.company_id, b1.gudong_name, b1.percent, b1.amount, b1.total, b1.gudong_id as erji_id,b2.gudong_name as erji_name, b2.percent as erji_percent, b2.amount as erji_amount, b2.total as erji_total, b2.gudong_id as sanji_id, b3.gudong_name as sanji_name, b3.percent as sanji_percent, b3.amount as sanji_amount, b3.total as sanji_total"
  	      +" from chigu_bili b1 left join chigu_bili b2 on b1.gudong_id = b2.company_id left join chigu_bili b3 on b2.gudong_id = b3.company_id")
  	  
//  	  val json = structure.mapGroups{case(companyId, it) => {
//  	    val yiji = Bili(company_id)
//  	  }}
  	  
  	  
//  	  println("------------result df of the province "+p+" is:"+ resultDF.count+"--------------")
//  	  DBUtil.saveDFToDB(resultDF, resultTable)
    }
	}
  
  
  
  val numCharSet = Set('0','1','2','3','4','5','6','7','8','9','.')
  
  def trimNumFromLeft(amountStr:String):Double = {
    val amount = "0"+amountStr.trim()
    for(i <- 0 until amount.length()){
      if(!numCharSet.contains(amount.charAt(i))){
        return amount.substring(0, i).toDouble
      }
    }
    
    amount.toDouble;
  }
  
}