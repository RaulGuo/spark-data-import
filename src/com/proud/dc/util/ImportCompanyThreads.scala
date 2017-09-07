package com.proud.dc.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.SparkSession
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.spark.storage.StorageLevel
import com.proud.ark.config.DBName
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.functions._

/**
 * 使用多线程的方式导入数据
 */
object ImportCompanyThreads {
  val prop = ImportUtil.getProperties
	val mode = SaveMode.Append
	val schema = "dc_import_append"
	
	def importCompanyRelatedWithDF(spark:SparkSession, province:String, provinceCode:Int, df:Dataset[Row], use207:Boolean = true, branch:Boolean = true, change:Boolean = true){
    var dbUrl:String = "";
	  if(use207)
	    dbUrl = ImportUtil.dbUrl
	  else
	    dbUrl = ImportUtil.dbUrl161
	    
    
	  println("-------------------------"+schema+"--------------")  
	  
    val prop = ImportUtil.getProperties
    val sqlContext = spark.sqlContext
    val start = System.currentTimeMillis()
    df.persist().createOrReplaceTempView("company")
    //最多四个线程一起运行
    val threadPool:ExecutorService=Executors.newFixedThreadPool(4)
    try {
      threadPool.execute(new ImportCompany(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyAbnormal(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyBranch(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyEquityPledge(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyStockInvestment(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMember(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyDebtSecured(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMortgageReg(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMortgageCollateral(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMortgage(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyPunishICBC(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanySpotCheck(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyStockHolder(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyEntChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyIntellectual(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyAicChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyInvestment(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyLicense(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyPunishEnt(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyStockChange(spark, province, provinceCode, dbUrl))
    }finally {
      threadPool.shutdown()
      threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
      val end = System.currentTimeMillis()
      df.unpersist()
      println("*************************take notice:********")
      println("import ***aaa"+province+"bb company by multithread cost time:"+(end-start)+"ms")
      println("----------------over-----------------------")
    }
  }
	
//  val dbUrl = ImportUtil.dbUrl
  def importCompanyRelated(spark:SparkSession, province:String, provinceCode:Int,  dstCompPrefix:String = "/home/data_center/dst/company/", use207:Boolean = true, branch:Boolean = true, change:Boolean = true){
    var dbUrl:String = "";
	  if(use207)
	    dbUrl = ImportUtil.dbUrl
	  else
	    dbUrl = ImportUtil.dbUrl161
	    
    
    val prop = ImportUtil.getProperties
    val sqlContext = spark.sqlContext
    val start = System.currentTimeMillis()
    val df:Dataset[Row] = sqlContext.read.json(dstCompPrefix+province).persist(StorageLevel.MEMORY_ONLY_SER)
    df.createOrReplaceTempView("company")
    //最多四个线程一起运行
    val threadPool:ExecutorService=Executors.newFixedThreadPool(4)
    try {
      threadPool.execute(new ImportCompany(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyAbnormal(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyBranch(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyEquityPledge(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyStockInvestment(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMember(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyDebtSecured(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMortgageReg(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMortgageCollateral(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyMortgage(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyPunishICBC(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanySpotCheck(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyStockHolder(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyEntChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyIntellectual(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyAicChange(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyInvestment(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyLicense(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyPunishEnt(spark, province, provinceCode, dbUrl))
      threadPool.execute(new ImportCompanyStockChange(spark, province, provinceCode, dbUrl))
    }finally {
      threadPool.shutdown()
      threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
      val end = System.currentTimeMillis()
      df.unpersist()
      println("*************************take notice:********")
      println("import ***aaa"+province+"bb company by multithread cost time:"+(end-start)+"ms")
      println("----------------over-----------------------")
    }
  }
  
  class ImportCompany(spark:SparkSession, province:String, provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val sc = spark.sparkContext
      sc.setLocalProperty("spark.scheduler.pool", "production")
      val compBaseDF = spark.sql("select company_id as id, bus.base.address,bus.base.check_at,bus.base.credit_no,bus.base.end_at,bus.base.formation,bus.base.leg_rep,bus.base.name,bus.base.reg_capi,bus.base.reg_no,bus.base.reg_org,bus.base.scope,bus.base.start_at,bus.base.state,bus.base.term_end_at,bus.base.term_start_at,bus.base.type from company").withColumn("md5", Util.genMd5Udf()).withColumn("province", lit(provinceCode))
//     compBaseDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company", prop)
      DBUtil.saveDFToDB(compBaseDF, schema+"."+province+"_company", mode)
    }
  }
  
  //bus.abnormals
  class ImportCompanyAbnormal(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val sc = spark.sparkContext
      sc.setLocalProperty("spark.scheduler.pool", "production")
      val abnormalDF = spark.sql("select company_id, bus.abnormals from company where bus.abnormals is not null")
      val abnormalRDD = abnormalDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("abnormals")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val abnormalStructType = abnormalDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val abnormalSchema = StructType(StructField("company_id", LongType, false) +: abnormalStructType.fields)
      val flatAbnormalDF = spark.createDataFrame(abnormalRDD, abnormalSchema).withColumn("province", lit(provinceCode))
//      flatAbnormalDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_abnormal", prop)
      DBUtil.saveDFToDB(flatAbnormalDF, schema+"."+province+"_company_abnormal", mode)
    }
  }
  
  //bus.branchs
  class ImportCompanyBranch(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val sc = spark.sparkContext
      sc.setLocalProperty("spark.scheduler.pool", "production")
      val branchDF = spark.sql("select company_id, bus.branchs from company where bus.branchs is not null")
      val branchRDD = branchDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("branchs")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      val branchStructType = branchDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val branchSchema = StructType(StructField("company_id", LongType, false) +: branchStructType.fields)
      val flatbranchDF = spark.createDataFrame(branchRDD, branchSchema).withColumn("province", lit(provinceCode))
//      flatbranchDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_branch", prop)
      DBUtil.saveDFToDB(flatbranchDF, schema+"."+province+"_company_branch", mode)
    }
  }
  
  //bus.changes
  /**
   * change表的before和after一般都比较大，需要设置成MEDIUMTEXT格式。
   * 但使用Overwrite的方式默认的建表方式用的是TEXT。所以最好自己建空表，然后append。
   */
  class ImportCompanyChange(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val sc = spark.sparkContext
      sc.setLocalProperty("spark.scheduler.pool", "production")
      val changeDF = spark.sql("select company_id, bus.changes as change from company where bus.changes is not null")
    	val changeRDD = changeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("change")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      val changeStructType = changeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val changeSchema = StructType(StructField("company_id", LongType, false) +: changeStructType.fields)
      val flatChangeDF = spark.createDataFrame(changeRDD, changeSchema).withColumn("province", lit(provinceCode))
//      flatChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, schema+"."+province+"_company_change", prop)
      DBUtil.saveDFToDB(flatChangeDF, schema+"."+province+"_company_change", mode)
    }
  }
  
  
  //bus.equity_pledges
  class ImportCompanyEquityPledge(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val equityPledgeDF = spark.sql("select company_id, bus.equity_pledges from company where bus.equity_pledges is not null")
      val equityPledgeRDD = equityPledgeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("equity_pledges")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val equityPledgeStructType = equityPledgeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val equityPledgeSchema = StructType(StructField("company_id", LongType, false) +: equityPledgeStructType.fields)
      val flatequityPledgeDF = spark.createDataFrame(equityPledgeRDD, equityPledgeSchema).withColumn("province", lit(provinceCode))
//      flatequityPledgeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_equity_pledge", prop)
      DBUtil.saveDFToDB(flatequityPledgeDF, schema+"."+province+"_company_equity_pledge", mode)
    }
  }
  
  //bus.investment
  //企业的股东及出资信息
  class ImportCompanyStockInvestment(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val compinvestmentDF = spark.sql("select company_id, bus.investment from company where bus.investment is not null")
      val compinvestmentRDD = compinvestmentDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("investment")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      val compinvestmentStructType = compinvestmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val compinvestmentSchema = StructType(StructField("company_id", LongType, false) +: compinvestmentStructType.fields)
      val flatcompinvestmentDF = spark.createDataFrame(compinvestmentRDD, compinvestmentSchema).withColumn("province", lit(provinceCode))
//      flatcompinvestmentDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_stock_investment", prop)
      DBUtil.saveDFToDB(flatcompinvestmentDF, schema+"."+province+"_company_stock_investment", mode)
    }
  }
  
  //bus.members
  class ImportCompanyMember(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
    val memberDF = spark.sql("select company_id, bus.members from company where bus.members is not null")
    val memberRDD = memberDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("members")
        val compId: Long = x.getAs[Long]("company_id")
        var list = List[Row]()
        rows.foreach { row => {
          val cols = row.toSeq
          val newRow = Row.fromSeq(compId+:cols)
          list = list:+newRow
        } }
        list.toArray[Row]
      }
    }

    val memberStructType = memberDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val memberSchema = StructType(StructField("company_id", LongType, false) +: memberStructType.fields)
    val flatmemberDF = spark.createDataFrame(memberRDD, memberSchema).withColumn("province", lit(provinceCode))
//    flatmemberDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_family_member", prop)
    DBUtil.saveDFToDB(flatmemberDF, schema+"."+province+"_company_family_member", mode)
    }
  }
  
  //bus.mortgages.debt_secured
  class ImportCompanyDebtSecured(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val mortgageDebtSecureDF = spark.sql("select company_id, bus.mortgages.debt_secured from company where bus.mortgages.debt_secured is not null")
      val mortgageDebtSecureRDD = mortgageDebtSecureDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("debt_secured")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val mortgageDebtSecureStructType = mortgageDebtSecureDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val mortgageDebtSecureSchema = StructType(StructField("company_id", LongType, false) +: mortgageDebtSecureStructType.fields)
      val flatmortgageDebtSecureDF = spark.createDataFrame(mortgageDebtSecureRDD, mortgageDebtSecureSchema).withColumnRenamed("type", "type_name").withColumn("province", lit(provinceCode))
//      flatmortgageDebtSecureDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_mortgage_debt_secure", prop)
      DBUtil.saveDFToDB(flatmortgageDebtSecureDF, schema+"."+province+"_company_mortgage_debt_secure", mode)
    }
  }
  
  //bus.mortgages.mortgage_reg
  class ImportCompanyMortgageReg(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val mortgageRegisterDF = spark.sql("select company_id, bus.mortgages.mortgage_reg from company where bus.mortgages.mortgage_reg is not null")
      val mortgageRegisterRDD = mortgageRegisterDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("mortgage_reg")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val mortgageRegisterStructType = mortgageRegisterDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val mortgageRegisterSchema = StructType(StructField("company_id", LongType, false) +: mortgageRegisterStructType.fields)
      val flatmortgageRegisterDF = spark.createDataFrame(mortgageRegisterRDD, mortgageRegisterSchema).withColumnRenamed("type", "type_name").withColumnRenamed("debut_type", "debt_type").withColumnRenamed("no", "register_num").withColumn("province", lit(provinceCode))
//      flatmortgageRegisterDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_mortgage_register", prop)
      DBUtil.saveDFToDB(flatmortgageRegisterDF, schema+"."+province+"_company_mortgage_register", mode)
    }
  }
  
  //bus.mortgages.collateral
  class ImportCompanyMortgageCollateral(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val mortgagecollateralDF = spark.sql("select company_id, bus.mortgages.collateral from company where bus.mortgages.collateral is not null")
      val mortgagecollateralRDD = mortgagecollateralDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[WrappedArray[Row]]]("collateral")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { rowArray => {
            if(rowArray != null){//一对多对多，所以需要两层迭代
            rowArray.foreach { row => {
              val cols = row.toSeq
              val newRow = Row.fromSeq(compId+:cols)
              list = list:+newRow
          }}}}}
          list.toArray[Row]
        }
      }
      val mortgagecollateralStructType = mortgagecollateralDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val mortgagecollateralSchema = StructType(StructField("company_id", LongType, false) +: mortgagecollateralStructType.fields)
      val flatmortgagecollateralDF = spark.createDataFrame(mortgagecollateralRDD, mortgagecollateralSchema).withColumn("province", lit(provinceCode))
//      flatmortgagecollateralDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_mortgage_collateral", prop)
      DBUtil.saveDFToDB(flatmortgagecollateralDF, schema+"."+province+"_company_mortgage_collateral", mode)
    }
  }
  
  //bus.mortgages.mortgagee
  class ImportCompanyMortgage(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val mortgageDF = spark.sql("select company_id, bus.mortgages.mortgagee from company where bus.mortgages.mortgagee is not null")
      val mortgageRDD = mortgageDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[WrappedArray[Row]]]("mortgagee")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { rowArray => {
            if(rowArray != null){
            rowArray.foreach { row => {
              val cols = row.toSeq
              val newRow = Row.fromSeq(compId+:cols)
              list = list:+newRow
          }}}}}
          list.toArray[Row]
        }
      }
      val mortgageStructType = mortgageDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val mortgageSchema = StructType(StructField("company_id", LongType, false) +: mortgageStructType.fields)
      val flatmortgageDF = spark.createDataFrame(mortgageRDD, mortgageSchema).withColumn("province", lit(provinceCode))
//      flatmortgageDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_mortgagee", prop)?
      DBUtil.saveDFToDB(flatmortgageDF, schema+"."+province+"_company_mortgagee", mode)
    }
  }
  
  //bus.punishs
  class ImportCompanyPunishICBC(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val punishDF = spark.sql("select company_id, bus.punishs from company where bus.punishs is not null")
      val punishRDD = punishDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("punishs")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val punishStructType = punishDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val punishSchema = StructType(StructField("company_id", LongType, false) +: punishStructType.fields)
      val flatpunishDF = spark.createDataFrame(punishRDD, punishSchema).withColumnRenamed("type", "type_name").withColumn("province", lit(provinceCode))
//      flatpunishDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_punish_icbc", prop)
      DBUtil.saveDFToDB(flatpunishDF, schema+"."+province+"_company_punish_icbc", mode)
    }
  }
  
  //bus.spot_checks
  class ImportCompanySpotCheck(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val spotCheckDF = spark.sql("select company_id, bus.spot_checks from company where bus.spot_checks is not null")
      val spotCheckRDD = spotCheckDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("spot_checks")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val spotCheckStructType = spotCheckDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val spotCheckSchema = StructType(StructField("company_id", LongType, false) +: spotCheckStructType.fields)
      val flatspotCheckDF = spark.createDataFrame(spotCheckRDD, spotCheckSchema).withColumnRenamed("type", "type_name").withColumn("province", lit(provinceCode))
//      flatspotCheckDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_spot_check", prop)
      DBUtil.saveDFToDB(flatspotCheckDF, schema+"."+province+"_company_spot_check", mode)
    }
  }
  
  //bus.stockholders
  class ImportCompanyStockHolder(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val stockholderDF = spark.sql("select company_id, bus.stockholders from company where bus.stockholders is not null")
      val stockholderRDD = stockholderDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("stockholders")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val stockholderStructType = stockholderDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val stockholderSchema = StructType(StructField("company_id", LongType, false) +: stockholderStructType.fields)
      val flatstockholderDF = spark.createDataFrame(stockholderRDD, stockholderSchema).withColumnRenamed("type", "type_name").withColumn("province", lit(provinceCode))
//      flatstockholderDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_stock_holder", prop)
      DBUtil.saveDFToDB(flatstockholderDF, schema+"."+province+"_company_stock_holder", mode)
    }
  }
  
  //ent.changes
  class ImportCompanyEntChange(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val entchangeDF = spark.sql("select company_id, ent.changes as change from company where ent.changes is not null")
    	val entchangeRDD = entchangeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("change")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val entchangeStructType = entchangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val entchangeSchema = StructType(StructField("company_id", LongType, false) +: entchangeStructType.fields)
      val flatentChangeDF = spark.createDataFrame(entchangeRDD, entchangeSchema).withColumn("province", lit(provinceCode))
//      flatentChangeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_ent_change", prop)
      DBUtil.saveDFToDB(flatentChangeDF, schema+"."+province+"_company_ent_change", mode)
    }
  }
  
  class ImportCompanyAicChange(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val aicchangeDF = spark.sql("select company_id, aic.changes as change from company where aic.changes is not null")
    	val aicchangeRDD = aicchangeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("change")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val aicchangeStructType = aicchangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val aicchangeSchema = StructType(StructField("company_id", LongType, false) +: aicchangeStructType.fields)
      val flataicChangeDF = spark.createDataFrame(aicchangeRDD, aicchangeSchema).withColumn("province", lit(provinceCode))
//      flataicChangeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"company_aic_change", prop)
      DBUtil.saveDFToDB(flataicChangeDF, schema+"."+province+"company_aic_change", mode)
    }
  }
  
  //ent.intellectuals
  class ImportCompanyIntellectual(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val intellectualDF = spark.sql("select company_id, ent.intellectuals from company where ent.intellectuals is not null")
      val intellectualRDD = intellectualDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("intellectuals")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
  
      val intellectualStructType = intellectualDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val intellectualSchema = StructType(StructField("company_id", LongType, false) +: intellectualStructType.fields)
      val flatintellectualDF = spark.createDataFrame(intellectualRDD, intellectualSchema).withColumnRenamed("no", "register_num").withColumn("province", lit(provinceCode))
//      flatintellectualDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_intellectual", prop)
      DBUtil.saveDFToDB(flatintellectualDF, schema+"."+province+"_company_intellectual", mode)
    }
  }
  
  //ent.investment
  class ImportCompanyInvestment(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val investmentDF = spark.sql("select company_id, ent.investment from company where ent.investment is not null")
      val investmentRDD = investmentDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("investment")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val investmentStructType = investmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val investmentSchema = StructType(StructField("company_id", LongType, false) +: investmentStructType.fields)
      val flatinvestmentDF = spark.createDataFrame(investmentRDD, investmentSchema).withColumn("province", lit(provinceCode))
//      flatinvestmentDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_contribution", prop)
      DBUtil.saveDFToDB(flatinvestmentDF, schema+"."+province+"_company_contribution", mode)
    }
  }
  
  
  //ent.licenses
  class ImportCompanyLicense(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val licenseDF = spark.sql("select company_id, ent.licenses from company where ent.licenses is not null")
      val licenseRDD = licenseDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("licenses")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val licenseStructType = licenseDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val licenseSchema = StructType(StructField("company_id", LongType, false) +: licenseStructType.fields)
      val flatlicenseDF = spark.createDataFrame(licenseRDD, licenseSchema).withColumnRenamed("no", "number").withColumn("province", lit(provinceCode))
//      flatlicenseDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_license", prop)
      DBUtil.saveDFToDB(flatlicenseDF, schema+"."+province+"_company_license", mode)
    }
  }
  
  
  //ent.punishs
  class ImportCompanyPunishEnt(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val comppunishDF = spark.sql("select company_id, ent.punishs from company where ent.punishs is not null")
      val comppunishRDD = comppunishDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("punishs")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val comppunishStructType = comppunishDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val comppunishSchema = StructType(StructField("company_id", LongType, false) +: comppunishStructType.fields)
      val flatcomppunishDF = spark.createDataFrame(comppunishRDD, comppunishSchema).withColumnRenamed("type", "type_name").withColumn("province", lit(provinceCode))
//      flatcomppunishDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_punish_ent", prop)
      DBUtil.saveDFToDB(flatcomppunishDF, schema+"."+province+"_company_punish_ent", mode)
    }
  }
  
  //ent.stock_changes
  class ImportCompanyStockChange(spark:SparkSession, province:String,provinceCode:Int,  dbUrl:String) extends Runnable{
    override def run(){
      val compstockChangeDF = spark.sql("select company_id, ent.stock_changes from company where ent.stock_changes is not null")
      val compstockChangeRDD = compstockChangeDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("stock_changes")
          val compId: Long = x.getAs[Long]("company_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }
      
      val compstockChangeStructType = compstockChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val compstockChangeSchema = StructType(StructField("company_id", LongType, false) +: compstockChangeStructType.fields)
      val flatcompstockChangeDF = spark.createDataFrame(compstockChangeRDD, compstockChangeSchema).withColumn("province", lit(provinceCode))
//      flatcompstockChangeDF.write.mode(mode).jdbc(dbUrl, schema+"."+province+"_company_stock_change", prop)?
      DBUtil.saveDFToDB(flatcompstockChangeDF, schema+"."+province+"_company_stock_change", mode)
    }
  }
  
  
}