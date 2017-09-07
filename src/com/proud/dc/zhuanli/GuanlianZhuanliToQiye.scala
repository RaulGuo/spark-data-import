package com.proud.dc.zhuanli

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import com.proud.ark.db.DBUtil
import com.proud.ark.date.DateUtil
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset
import java.sql.Date
import com.proud.ark.config.ConfigUtil
import com.proud.ark.config.DBName
import com.proud.ark.data.HDFSUtil

/**
将专利权人关联到企业，分别要计算每个公司最新的专利信息（根据专利申请日来比较最新的一条）和每个专利对应的多个公司信息（根据专利权人来关联）
要注意这里要保存的有两个表（patent.zhuanli_quanren和patent.zhuanli_gongsi_xinxi），取决于最后要计算的结果，需要将保存记录的操作关掉或打开。
nohup spark-submit --executor-memory 10g --master spark://bigdata01:7077 --class com.proud.dc.zhuanli.GuanlianZhuanliToQiye --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
 */

object GuanlianZhuanliToQiye {
  case class ZhuanliQuanren(zhuanli_id:Long, zhuanli_quanren:String, tag:String, shenqingri:Date, mingcheng:String, md5:String)
  case class ZhuanliGongsi(company_id:Long, md5:String, zhuanli_zongshu:Long, waiguansheji:Long, famingshouquan:Long, famingshending:Long, faminggongbu:Long, shiyongxinxing:Long, last_zhuanli_id:Long, last_zhuanli_name:String, last_zhuanli_shenqingri:Date)
  case class ZhuanliShuliang(company_id:Long, zhuanli_zongshu:Long, waiguansheji:Long, famingzhuanli:Long, shiyongxinxing:Long)
  case class LastZhuanli(company_id:Long, company_md5:String, last_zhuanli_id:Long, last_zhuanli_name:String, last_zhuanli_shenqingri:Date, last_zhuanli_md5:String)
  
  val waiguanshejiStr = "外观设计"
  val famingzhuanliStr = "发明专利"
  val shiyongxinxingStr = "实用新型"
  
  def main(args: Array[String]): Unit = {
    val resultTable = s"${DBName.patent}.zhuanli_gongsi_xinxi"
    DBUtil.truncate(resultTable)
    DBUtil.truncate(s"${DBName.patent}.zhuanli_quanren")
    
	  val spark = SparkSession.builder().master(ConfigUtil.master).appName("GuanliZhuanliToQiye").config("spark.sql.warehouse.dir", ConfigUtil.warehouse)
	  .master(ConfigUtil.master).getOrCreate()
	  
	  val sc = spark.sparkContext
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  
	  //将zhuanli_quanren拆成多个值，并把shenqingri字段转换为日期，从而将一条记录组装成多个对象。
	  val zhuanliDF = DBUtil.loadDFFromTable("patent.zhuanli_xinxi", spark).select("id", "zhuanli_quanren", "tag", "shenqingri", "mingcheng", "md5").filter(row => row.getAs[String]("zhuanli_quanren") != null)
	  .flatMap { row => {
	    val quanrens = row.getAs[String]("zhuanli_quanren").split(";")
	    val id = row.getAs[Long]("id")
	    val tag = row.getAs[String]("tag")
	    val shenqingri = DateUtil.transStringToDate(row.getAs[String]("shenqingri"))
	    val mingcheng = row.getAs[String]("mingcheng")
	    val md5 = row.getAs[String]("md5")
	    quanrens.map { x => new ZhuanliQuanren(id, x.trim(), tag, shenqingri, mingcheng, md5) }
	  } }
	  
	  //表中数据：zhuanli_id, zhuanli_quanren, tag, shenqingri, mingcheng
	  zhuanliDF.createOrReplaceTempView("zhuanli")
	  
	  //获得企业数据(id, name, md5)
	  val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark)
	  companyDF.createOrReplaceTempView("company")
	  
	  val zhuanliQuanrenSQL = "select c.id as company_id, c.name as company_name, z.zhuanli_id, z.zhuanli_quanren, c.md5 as company_md5, z.tag, z.shenqingri, z.mingcheng, z.md5 as zhuanli_md5 from zhuanli z inner join company c on z.zhuanli_quanren = c.name"
	  val zhuanliCompanyDF = spark.sql(zhuanliQuanrenSQL).persist(StorageLevel.MEMORY_AND_DISK)
    DBUtil.saveDFToDB(zhuanliCompanyDF.select("company_id", "zhuanli_id", "zhuanli_quanren", "company_md5"), s"${DBName.patent}.zhuanli_quanren")
    
    zhuanliCompanyDF.createOrReplaceTempView("zhuanli_company")
    
    //1. 统计每个公司对应的专利总数以及各种专利的数量
    val zhuanliZongshuSQL = "select count(*) as count, tag, company_id from zhuanli_company zc group by company_id, tag"
    val zhuanliZongshuDF = spark.sql(zhuanliZongshuSQL)
    val zongshuDS:Dataset[ZhuanliShuliang] = zhuanliZongshuDF.groupByKey { x => x.getAs[Long]("company_id") }.mapGroups(composeZhuanliZongshu)
    
    //2. 统计每个公司的最新的专利信息
    //先按照company_id分组，分组后使用组内信息进行reduce
    val zuixinZhuanliDS = zhuanliCompanyDF.groupByKey { x => x.getAs[Long]("company_id") }.mapGroups[LastZhuanli]((id:Long, it:Iterator[Row]) => {
      val r = it.reduce(reduceZhuanliByShenqingri)
      new LastZhuanli(id, r.getAs("company_md5"), r.getAs("zhuanli_id"), r.getAs("mingcheng"), r.getAs("shenqingri"), r.getAs("zhuanli_md5"))
    })
    
    //test
    val df = zhuanliCompanyDF.groupByKey { x => x.getAs[Long]("company_id") }.reduceGroups((r1:Row, r2:Row) => reduceZhuanliByShenqingri(r1, r2))
    
    val result = zuixinZhuanliDS.join(zongshuDS, "company_id")
    
    DBUtil.saveDFToDB(result, resultTable)
  }

  //定义一个方法，用于根据企业id对专利分组后，从企业对应的一组专利中reduce出最新的一条记录（根据shenqingri字段来判断）
  def reduceZhuanliByShenqingri(r1:Row, r2:Row):Row = {
    val shenqingri1 = r1.getAs[Date]("shenqingri")
    val shenqingri2 = r2.getAs[Date]("shenqingri")
    if(shenqingri1 == null)
      r2
    else if(shenqingri2 == null)
      r1
    else if(shenqingri1.after(shenqingri2))
      r1
    else 
      r2
  }
    
  
  //定义方法，在专利根据公司ID和类型分组后，将group的结果转换成一个统计公司各种专利数量的对象
  //专利的几种分类：外观设计，发明专利，实用新型
  //row当中的记录包括：count, tag, company_id
  def composeZhuanliZongshu(companyId:Long, it:Iterator[Row]):ZhuanliShuliang = {
    
    var waiguansheji:Long = 0
    var famingzhuanli:Long = 0
    var shiyongxinxing:Long = 0
    var zongshu:Long = 0
    
    for(r <- it){
      val fenlei = r.getAs[String]("tag")
      val num = r.getAs[Long]("count")
      zongshu = zongshu+num
      if(fenlei == waiguanshejiStr)
        waiguansheji = num
      else if(fenlei == famingzhuanliStr)
        famingzhuanli = num
      else if(fenlei == shiyongxinxingStr)
        shiyongxinxing = num
    }
    
    new ZhuanliShuliang(companyId, zongshu, waiguansheji, famingzhuanli, shiyongxinxing)
  }
}