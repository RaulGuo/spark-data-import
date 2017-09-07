package com.proud.dc.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.SparkSession
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.Dataset
import com.proud.ark.config.DBName
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.functions._
/**
 * 使用多线程的方式导入数据
 */
object ImportReportThreads {
  val prop = ImportUtil.getProperties
  val schema = "dc_import"
  val mode = SaveMode.Append
  
  def importReportRelatedWithDF(spark:SparkSession, province:String, provinceCode:Int, reportEntityDF:Dataset[Row],use207:Boolean = true, base:Boolean = true){
	  var dbUrl:String = "";
	  if(use207)
	    dbUrl = ImportUtil.dbUrl
	  else
	    dbUrl = ImportUtil.dbUrl161
    val prop = ImportUtil.getProperties
    val sqlContext = spark.sqlContext
    val start = System.currentTimeMillis()
    reportEntityDF.persist().createOrReplaceTempView("report")
    val threadPool:ExecutorService=Executors.newFixedThreadPool(10)
    try {
      threadPool.execute(new ImportReportBase(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportBranch(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportGuarantee(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportInvEnt(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportInvestment(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportLicense(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportOperation(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportStockChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportWebsite(spark, province, provinceCode, dbUrl))
      
    }finally {
      threadPool.shutdown()
      threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
      val end = System.currentTimeMillis()
      reportEntityDF.unpersist()
      println("*************************take notice:********")
      println("import cccc"+province+"--ddd report by multithread cost time:"+(end-start)+"ms")
      println("----------------over-----------------------")
    }
  }
  
  
  
  def importReportRelated(spark:SparkSession, province:String, provinceCode:Int, dstReportPrefix:String = "/home/data_center/dst/report/",use207:Boolean = true, base:Boolean = true){
	  var dbUrl:String = "";
	  if(use207)
	    dbUrl = ImportUtil.dbUrl
	  else
	    dbUrl = ImportUtil.dbUrl161
    val prop = ImportUtil.getProperties
    val sqlContext = spark.sqlContext
    val start = System.currentTimeMillis()
    val reportEntityDF = sqlContext.read.json(dstReportPrefix+province).cache()
    reportEntityDF.createOrReplaceTempView("report")
    val threadPool:ExecutorService=Executors.newFixedThreadPool(10)
    try {
      threadPool.execute(new ImportReportBase(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportBranch(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportGuarantee(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportInvEnt(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportInvestment(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportLicense(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportOperation(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportStockChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportReportWebsite(spark, province, provinceCode, dbUrl))
      
    }finally {
      threadPool.shutdown()
      threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
      val end = System.currentTimeMillis()
      reportEntityDF.unpersist()
      println("*************************take notice:********")
      println("import cccc"+province+"--ddd report by multithread cost time:"+(end-start)+"ms")
      println("----------------over-----------------------")
    }
  }
  
  class ImportReportBase(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      println("-----------import report base-------------------")
      //完成的导入报表
//      val reportBaseDF = spark.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_guarantee ,ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, ent_base.type as type_name, report_at, year from report")
      //缺少is_guarantee
//      val reportBaseDF = spark.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, ent_base.type as type_name, report_at, year from report")
      //缺少is_guarantee和type
      val reportBaseDF = spark.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, report_at, year from report").withColumn("md5", Util.genMd5Udf()).withColumn("province", lit(provinceCode))
//      reportBaseDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_base", prop)
      DBUtil.saveDFToDB(reportBaseDF, schema+"."+province+"_company_report_base", mode)
    }
  }
  
  class ImportReportBranch(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val reportbranchDF = spark.sql("select report_id, branchs from report where branchs is not null")
      val reportbranchRDD = reportbranchDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("branchs")
        val reportId: Long = x.getAs[Long]("report_id")
        var list = List[Row]()
        rows.foreach { row => {
          val cols = row.toSeq
          val newRow = Row.fromSeq(reportId+:cols)
          list = list:+newRow
        } }
        list.toArray[Row]
        }
      }
    
      val reportbranchStructType = reportbranchDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val reportbranchSchema = StructType(StructField("report_id", LongType, false) +: reportbranchStructType.fields)
      val flatreportbranchDF = spark.createDataFrame(reportbranchRDD, reportbranchSchema).withColumn("province", lit(provinceCode))
//      flatreportbranchDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_branch", prop)
      DBUtil.saveDFToDB(flatreportbranchDF, schema+"."+province+"_company_report_branch", mode)
    }
  }
  
  class ImportReportChange(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val reportChangeDF = spark.sql("select report_id, changes from report where changes is not null")
      val reportChangeRDD = reportChangeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("changes")
        val reportId: Long = x.getAs[Long]("report_id")
        var list = List[Row]()
        rows.foreach { row => {
          val cols = row.toSeq
          val newRow = Row.fromSeq(reportId+:cols)
          list = list:+newRow
        } }
        list.toArray[Row]
        }
      }
    
      val reportchangeStructType = reportChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val reportchangeSchema = StructType(StructField("report_id", LongType, false) +: reportchangeStructType.fields)
      val flatreportchangeDF = spark.createDataFrame(reportChangeRDD, reportchangeSchema).withColumn("province", lit(provinceCode))
//      flatreportchangeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_change", prop)
      DBUtil.saveDFToDB(flatreportchangeDF, schema+"."+province+"_company_report_change", mode)
    }
  }
  
  //ent.reports.guarantees
  class ImportReportGuarantee(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val reportGuaranteeDF = spark.sql("select report_id, guarantees from report where guarantees is not null")
      val reportGuaranteeRDD = reportGuaranteeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("guarantees")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val reportGuaranteeStructType = reportGuaranteeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val reportGuaranteeSchema = StructType(StructField("report_id", LongType, false) +: reportGuaranteeStructType.fields)
      val flatreportGuaranteeDF = spark.createDataFrame(reportGuaranteeRDD, reportGuaranteeSchema).withColumn("province", lit(provinceCode))
//      flatreportGuaranteeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_guarantee", prop)
      DBUtil.saveDFToDB(flatreportGuaranteeDF, schema+"."+province+"_company_report_guarantee", mode)
    }
  }
  
  //ent.reports.inv_ents
  class ImportReportInvEnt(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val invEntsDF = spark.sql("select report_id, inv_ents from report where inv_ents is not null")
      val invEntsRDD = invEntsDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("inv_ents")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val invEntsStructType = invEntsDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val invEntsSchema = StructType(StructField("report_id", LongType, false) +: invEntsStructType.fields)
      val flatinvEntsDF = spark.createDataFrame(invEntsRDD, invEntsSchema).withColumn("province", lit(provinceCode))
//      flatinvEntsDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_invest_enterprise", prop)
      DBUtil.saveDFToDB(flatinvEntsDF, schema+"."+province+"_company_report_invest_enterprise", mode)
    }
  }
  
  //ent.reports.investment
  class ImportReportInvestment(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val reportInvestmentDF = spark.sql("select report_id, investment from report where investment is not null")
      val reportInvestmentRDD = reportInvestmentDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("investment")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val reportInvestmentStructType = reportInvestmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val reportInvestmentSchema = StructType(StructField("report_id", LongType, false) +: reportInvestmentStructType.fields)
      val flatreportinvestmentDF = spark.createDataFrame(reportInvestmentRDD, reportInvestmentSchema).withColumn("province", lit(provinceCode))
//      flatreportinvestmentDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_stock_investment", prop)
      DBUtil.saveDFToDB(flatreportinvestmentDF, schema+"."+province+"_company_report_stock_investment", mode)
    }
  }
  
  
  class ImportReportLicense(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
    val licenceDF = spark.sql("select report_id, licenses from report where licenses is not null")
    val licenceRDD = licenceDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("licenses")
        val reportId: Long = x.getAs[Long]("report_id")
        var list = List[Row]()
        rows.foreach { row => {
          val cols = row.toSeq
          val newRow = Row.fromSeq(reportId+:cols)
          list = list:+newRow
        } }
        list.toArray[Row]
      }
    }
    val licenceStructType = licenceDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val licenceSchema = StructType(StructField("report_id", LongType, false) +: licenceStructType.fields)
    val flatlicenceDF = spark.createDataFrame(licenceRDD, licenceSchema).withColumn("province", lit(provinceCode))
//    flatlicenceDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_licence", prop)
    DBUtil.saveDFToDB(flatlicenceDF, schema+"."+province+"_company_report_licence", mode)
    }
  }
  
  
  //ent.reports.operation
  class ImportReportOperation(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
    val operationDF = spark.sql("select report_id, operation from report where operation is not null")
    val operationRDD = operationDF.rdd.map{
      x => {
        val row = x.getAs[Row]("operation")
        val reportId: Long = x.getAs[Long]("report_id")
        val cols = row.toSeq
        val newRow = Row.fromSeq(reportId+:cols)
        newRow
      }
    }

    val operationStructType = operationDF.schema.fields(1).dataType.asInstanceOf[StructType]
    val operationSchema = StructType(StructField("report_id", LongType, false) +: operationStructType.fields)
    val flatoperationDF = spark.createDataFrame(operationRDD, operationSchema).withColumnRenamed("fund_subsidy", "gover_subsidy").withColumn("province", lit(provinceCode))
//    flatoperationDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_operation", prop)
    DBUtil.saveDFToDB(flatoperationDF, schema+"."+province+"_company_report_operation", mode)
    }
  }
  
  
  //ent.reports.stock_changes
  class ImportReportStockChange(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val stockChangeDF = spark.sql("select report_id, stock_changes from report where stock_changes is not null")
      val stockChangeRDD = stockChangeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("stock_changes")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val stockChangeStructType = stockChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val stockChangeSchema = StructType(StructField("report_id", LongType, false) +: stockChangeStructType.fields)
      val flatstockChangeDF = spark.createDataFrame(stockChangeRDD, stockChangeSchema).withColumn("province", lit(provinceCode))
//      flatstockChangeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_stock_change", prop)
      DBUtil.saveDFToDB(flatstockChangeDF, schema+"."+province+"_company_report_stock_change", mode)
    }
  }
  
  
  class ImportReportWebsite(spark:SparkSession, province:String, provinceCode:Int, dbUrl:String) extends Runnable{
    override def run(){
      val websiteDF = spark.sql("select report_id, websites from report where websites is not null")
      val websiteRDD = websiteDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("websites")
        val reportId: Long = x.getAs[Long]("report_id")
        var list = List[Row]()
        rows.foreach { row => {
          val cols = row.toSeq
          val newRow = Row.fromSeq(reportId+:cols)
          list = list:+newRow
        } }
        list.toArray[Row]
      }
    }
    val websiteStructType = websiteDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val websiteSchema = StructType(StructField("report_id", LongType, false) +: websiteStructType.fields)
    val flatwebsiteDF = spark.createDataFrame(websiteRDD, websiteSchema).withColumnRenamed("type", "type_name").withColumn("province", lit(provinceCode))
//    flatwebsiteDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_report_website", prop)
    DBUtil.saveDFToDB(flatwebsiteDF, schema+"."+province+"_company_report_website", mode)
      
    }
  }
  
  
  
}