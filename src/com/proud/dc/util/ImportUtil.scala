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
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import com.proud.ark.data.HDFSUtil

object ImportUtil {
  val dbUrl = "jdbc:mysql://192.168.1.207:3306/test?autoReconnect=true"
  val jobsdbUrl = "jdbc:mysql://192.168.1.207:3306/jobs?autoReconnect=true"
  
  val dbUrl161 = "jdbc:mysql://192.168.1.161:3306/dc_import?autoReconnect=true"
  def getProperties(): Properties = {
	  var properties = new Properties()
	  properties.put("user", "root")
	  properties.put("password", "PS@Letmein123")
	  properties.put("driver", "com.mysql.jdbc.Driver")
	  properties.put("maxActive", "50")
	  properties.put("useServerPrepStmts", "false")
	  properties.put("rewriteBatchedStatements","true")
	  properties
  }
  
  def getProperties161(): Properties = {
	  var properties = new Properties()
	  properties.put("user", "root")
	  properties.put("password", "root")
	  properties.put("driver", "com.mysql.jdbc.Driver")
	  properties.put("maxActive", "50")
	  properties.put("useServerPrepStmts", "false")
	  properties.put("rewriteBatchedStatements","true")
	  properties
  }
  
  def loadDFFromTable(table: String, spark:SparkSession) = {
    val df = spark.read.format("jdbc").option("url", dbUrl).option("dbtable", table).option("user", "root").option("password", "PS@Letmein123").option("driver", "com.mysql.jdbc.Driver").load()
    df
  }
  
  
  val sourcePrefix = "/home/data_center/source/"
  val dstCompPrefix = "/home/data_center/dst/company/"
  val dstReportPrefix = "/home/data_center/dst/report/"
  
  def zipDataFrameWithId(df:Dataset[Row], sc:SparkContext, sqlContext:SQLContext, idOffset:Long = 0, colName:String = "id"):Dataset[Row] = {
    val schema = df.schema
    val newSchema = schema.add(colName, LongType)
    
    val result = df.rdd.zipWithIndex().map{case (row, id) => {
      val cols = row.toSeq
      val newRow = Row.fromSeq(cols.:+(id+idOffset))
      newRow
    }}
  
    sqlContext.createDataFrame(result, newSchema)
  }
  
  /**
   * 对saveToLocal的重写
   * 不把文件写入本地，而是直接返回对应的dataframe
   * sourceName是要处理后保存的数据的文件名
   * dstName是处理后保存的数据的文件名
   * idOffset是指公司id要添加多少（防止重复）
   * reportIdOffset是指报表ID要添加多少
   */
  def addIdToData(sc:SparkContext, sourceName:String, compIdOffset:Long = 1L, reportIdOffset:Long = 1L
      ,sourcePrefix:String = HDFSUtil.hdfsUrl+"/home/data_center/source/"):(Dataset[Row], Dataset[Row]) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val origDF = sqlContext.read.json(sourcePrefix+sourceName)
    
    import sqlContext.implicits._
    addIdToDF(sc, origDF, compIdOffset, reportIdOffset)
    
//    val df = origDF.withColumn("company_id",  monotonically_increasing_id+compIdOffset)
//    val df = zipDataFrameWithId(origDF, sc, sqlContext, compIdOffset, "company_id")
//    df.createOrReplaceTempView("company_save")
//    
//    val reportDF = sqlContext.sql("select company_id, ent.reports as report from company_save where ent.reports is not null")
//    
//    val fields = reportDF.schema.fields
//    val second = fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val reportSchema = StructType(StructField("company_id", LongType, false)+: second.fields)
//    val encoder = RowEncoder(reportSchema)
//    
//  	val reportEntity:Dataset[Row] = reportDF.flatMap {
//  	  x => {
//  	    //先把report从表中取出来，此时的结构是一个company_id对应多个row
//  	    val rows = x.getAs[WrappedArray[Row]]("report")
//  	    val compId:Long = x.getAs[Long]("company_id")
//  	    
//  	    val allRows = rows.map { row => {
//  	      val cols = row.toSeq
//  	      val newRow = Row.fromSeq(compId+:cols)
//  	      newRow
//  	    }}
//  	    allRows
//  	  }
//  	}(encoder)
//  	
////  	val reportEntityDF = reportEntity.withColumn("report_id", monotonically_increasing_id+reportIdOffset)
//    val reportEntityDF = zipDataFrameWithId(reportEntity, sc, sqlContext, reportIdOffset, "report_id")
//    (df, reportEntityDF)
  }
  
  
  def addIdToDF(sc:SparkContext, origDF:Dataset[Row], compIdOffset:Long = 1L, reportIdOffset:Long = 1L):(Dataset[Row], Dataset[Row]) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val df = zipDataFrameWithId(origDF, sc, sqlContext, compIdOffset, "company_id")
    df.createOrReplaceTempView("company_save")
    
    val reportDF = sqlContext.sql("select company_id, ent.reports as report from company_save where ent.reports is not null")
    
    val fields = reportDF.schema.fields
    val second = fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val reportSchema = StructType(StructField("company_id", LongType, false)+: second.fields)
    val encoder = RowEncoder(reportSchema)
    
  	val reportEntity:Dataset[Row] = reportDF.flatMap {
  	  x => {
  	    //先把report从表中取出来，此时的结构是一个company_id对应多个row
  	    val rows = x.getAs[WrappedArray[Row]]("report")
  	    val compId:Long = x.getAs[Long]("company_id")
  	    
  	    val allRows = rows.map { row => {
  	      val cols = row.toSeq
  	      val newRow = Row.fromSeq(compId+:cols)
  	      newRow
  	    }}
  	    allRows
  	  }
  	}(encoder)
  	
    val reportEntityDF = zipDataFrameWithId(reportEntity, sc, sqlContext, reportIdOffset, "report_id")
    (df, reportEntityDF)
  }
  
  /**
   * sourceName是要处理后保存的数据的文件名
   * dstName是处理后保存的数据的文件名
   * idOffset是指公司id要添加多少（防止重复）
   * reportIdOffset是指报表ID要添加多少
   */
  def saveToLocal(sc:SparkContext, sourceName:String, dstName:String, compIdOffset:Long = 1L, reportIdOffset:Long = 1L
      ,sourcePrefix:String = "/home/data_center/source/", dstCompPrefix:String = "/home/data_center/dst/company/", 
      dstReportPrefix:String = "/home/data_center/dst/report/"){
    val sqlContext = new SQLContext(sc)
    val origDF = sqlContext.read.json(sourcePrefix+sourceName)
    
    val rows = origDF.rdd.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(r.toSeq :+ (id+compIdOffset))
    }
    
    val df = sqlContext.createDataFrame(rows, StructType(origDF.schema.fields :+ StructField("company_id", LongType, false))).cache()
    
    df.createOrReplaceTempView("company_save")
    df.write.mode(SaveMode.Overwrite).json(dstCompPrefix+dstName)
    
    
    val reportDF = sqlContext.sql("select company_id, ent.reports as report from company_save where ent.reports is not null")
  	val reportRDD = reportDF.rdd
  	val reportEntity = reportRDD.flatMap{
  	  x => {
  	    //先把report从表中取出来，此时的结构是一个company_id对应多个row
  	    val rows = x.getAs[WrappedArray[Row]]("report")
  	    val compId: Long = x.getAs[Long]("company_id")
  	    
  	    var list = List[Row]()
  	    //对rows做循环，每个row中都是一条Report实体
  	    rows.foreach { row => {
  	      val cols = row.toSeq
  	      val newRow = Row.fromSeq(compId+:cols)
  	      list = newRow+:list
  	    } }
  	    list.toArray[Row]
  	  }
  	}.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq((id+reportIdOffset)+:r.toSeq)
    }
  	
  	val fields = reportDF.schema.fields
  	val second = fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
  	val reportSchema = StructType(StructField("report_id", LongType, false) +:StructField("company_id", LongType, false)+: second.fields)
  	val reportEntityDF = sqlContext.createDataFrame(reportEntity, reportSchema)
    reportEntityDF.write.mode(SaveMode.Overwrite).json(dstReportPrefix+dstName)
    
    df.unpersist()
  }
  
  
  
  
  /**
   * 导入公司以及公司相关联的数据
   */
  def importCompanyRelated(sc:SparkContext, dstName:String){
    val prop = getProperties
    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.json(dstCompPrefix+dstName).cache()
    
    df.createOrReplaceTempView("company")
    
    val compBaseDF = sqlContext.sql("select company_id as id, bus.base.address,bus.base.check_at,bus.base.credit_no,bus.base.end_at,bus.base.formation,bus.base.leg_rep,bus.base.name,bus.base.reg_capi,bus.base.reg_no,bus.base.reg_org,bus.base.scope,bus.base.start_at,bus.base.state,bus.base.term_end_at,bus.base.term_start_at,bus.base.type from company")
    compBaseDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company", prop)
    
    //bus.abnormals的处理
    val abnormalDF = sqlContext.sql("select company_id, bus.abnormals from company where bus.abnormals is not null")
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
    val flatAbnormalDF = sqlContext.createDataFrame(abnormalRDD, abnormalSchema)
    flatAbnormalDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_abnormal", prop)
    
    
    //bus.branchs
    val branchDF = sqlContext.sql("select company_id, bus.branchs from company where bus.branchs is not null")
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
    val flatbranchDF = sqlContext.createDataFrame(branchRDD, branchSchema)
    flatbranchDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_branch", prop)
    
    
    //bus.changes
    val changeDF = sqlContext.sql("select company_id, bus.changes as change from company where bus.changes is not null")
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
    val flatChangeDF = sqlContext.createDataFrame(changeRDD, changeSchema)
    flatChangeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_change", prop)
    
    
    //bus.equity_pledges
    val equityPledgeDF = sqlContext.sql("select company_id, bus.equity_pledges from company where bus.equity_pledges is not null")
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
    val flatequityPledgeDF = sqlContext.createDataFrame(equityPledgeRDD, equityPledgeSchema)
    flatequityPledgeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_equity_pledge", prop)
    
    
    //bus.investment
    //企业的股东及出资信息
    val compinvestmentDF = sqlContext.sql("select company_id, bus.investment from company where bus.investment is not null")
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
    val flatcompinvestmentDF = sqlContext.createDataFrame(compinvestmentRDD, compinvestmentSchema)
    flatcompinvestmentDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_stock_investment", prop)
    
    
    //bus.members
    val memberDF = sqlContext.sql("select company_id, bus.members from company where bus.members is not null")
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
    val flatmemberDF = sqlContext.createDataFrame(memberRDD, memberSchema)
    flatmemberDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_family_member", prop)
    
    //bus.mortgages.debt_secured
    val mortgageDebtSecureDF = sqlContext.sql("select company_id, bus.mortgages.debt_secured from company where bus.mortgages.debt_secured is not null")
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
    val flatmortgageDebtSecureDF = sqlContext.createDataFrame(mortgageDebtSecureRDD, mortgageDebtSecureSchema).withColumnRenamed("type", "type_name")
    flatmortgageDebtSecureDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_mortgage_debt_secure", prop)
    
    
    //bus.mortgages.mortgage_reg
    val mortgageRegisterDF = sqlContext.sql("select company_id, bus.mortgages.mortgage_reg from company where bus.mortgages.mortgage_reg is not null")
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
    val flatmortgageRegisterDF = sqlContext.createDataFrame(mortgageRegisterRDD, mortgageRegisterSchema).withColumnRenamed("type", "type_name").withColumnRenamed("no", "register_num").withColumnRenamed("debut_type", "debt_type")
    flatmortgageRegisterDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_mortgage_register", prop)
    
    
    //bus.mortgages.collateral
    val mortgagecollateralDF = sqlContext.sql("select company_id, bus.mortgages.collateral from company where bus.mortgages.collateral is not null")
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
    val flatmortgagecollateralDF = sqlContext.createDataFrame(mortgagecollateralRDD, mortgagecollateralSchema)
    flatmortgagecollateralDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_mortgage_collateral", prop)
    
    
    //bus.mortgages.mortgagee
    val mortgageDF = sqlContext.sql("select company_id, bus.mortgages.mortgagee from company where bus.mortgages.mortgagee is not null")
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
    val flatmortgageDF = sqlContext.createDataFrame(mortgageRDD, mortgageSchema)
    flatmortgageDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_mortgagee", prop)
    
    
    
    
    //bus.punishs
    val punishDF = sqlContext.sql("select company_id, bus.punishs from company where bus.punishs is not null")
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
    val flatpunishDF = sqlContext.createDataFrame(punishRDD, punishSchema).withColumnRenamed("type", "type_name")
    flatpunishDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_punish_icbc", prop)
    
    
    //bus.spot_checks
    val spotCheckDF = sqlContext.sql("select company_id, bus.spot_checks from company where bus.spot_checks is not null")
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
    val flatspotCheckDF = sqlContext.createDataFrame(spotCheckRDD, spotCheckSchema).withColumnRenamed("type", "type_name")
    flatspotCheckDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_spot_check", prop)
    
    
    //bus.stockholders
    val stockholderDF = sqlContext.sql("select company_id, bus.stockholders from company where bus.stockholders is not null")
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
    val flatstockholderDF = sqlContext.createDataFrame(stockholderRDD, stockholderSchema).withColumnRenamed("type", "type_name")
    flatstockholderDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_stock_holder", prop)
    
    
    //ent.changes
    val entchangeDF = sqlContext.sql("select company_id, ent.changes as change from company where ent.changes is not null")
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
    val flatentChangeDF = sqlContext.createDataFrame(entchangeRDD, entchangeSchema)
    flatentChangeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_ent_change", prop)
    
    
    
    //ent.intellectuals
    val intellectualDF = sqlContext.sql("select company_id, ent.intellectuals from company where ent.intellectuals is not null")
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
    val flatintellectualDF = sqlContext.createDataFrame(intellectualRDD, intellectualSchema).withColumnRenamed("no", "register_num")
    flatintellectualDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_intellectual", prop)
    
    
    
    //ent.investment
    val investmentDF = sqlContext.sql("select company_id, ent.investment from company where ent.investment is not null")
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
    val flatinvestmentDF = sqlContext.createDataFrame(investmentRDD, investmentSchema)
    flatinvestmentDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_contribution", prop)
    
    
    
    //ent.licenses
    val licenseDF = sqlContext.sql("select company_id, ent.licenses from company where ent.licenses is not null")
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
    val flatlicenseDF = sqlContext.createDataFrame(licenseRDD, licenseSchema).withColumnRenamed("no", "number")
    flatlicenseDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_license", prop)
    
    
    //ent.punishs
    val comppunishDF = sqlContext.sql("select company_id, ent.punishs from company where ent.punishs is not null")
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
    val flatcomppunishDF = sqlContext.createDataFrame(comppunishRDD, comppunishSchema).withColumnRenamed("type", "type_name")
    flatcomppunishDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_punish_ent", prop)
    
    
    
    //ent.stock_changes
    val compstockChangeDF = sqlContext.sql("select company_id, ent.stock_changes from company where ent.stock_changes is not null")
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
    val flatcompstockChangeDF = sqlContext.createDataFrame(compstockChangeRDD, compstockChangeSchema)
    flatcompstockChangeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_stock_change", prop)
    
    df.unpersist()
  }
  
  
  def importReportRelated(sc:SparkContext, dstName:String, dstReportPrefix1:String =dstReportPrefix){
    val prop = getProperties
    val sqlContext = new SQLContext(sc)
    
    val reportEntityDF = sqlContext.read.json(dstReportPrefix1+dstName).cache()
    reportEntityDF.createOrReplaceTempView("report")
    //telphone => telephone, type => type_name
//    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_guarantee ,ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, ent_base.type as type_name, report_at, year from report")
    
    //广西的报表没有is_guarantee和type
//    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, report_at, year from report")
    
    
    //河北的报表没有is_guarantee
//    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, ent_base.type as type_name, report_at, year from report")
    
    
    //山东的报表没有is_guarantee和type
//    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, report_at, year from report")
    
    
    //天津的报表没有is_guarantee
//    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, report_at, year from report")
    
    //西藏的报表没有is_guarantee
    
    //云南的报表没有is_guarantee
    
    
    //湖北的报表没有is_guarantee
    
    //湖南的报表没有is_guarantee
    
    //河南的报表没有is_guarantee
    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone,ent_base.type as type_name, report_at, year from report")
    reportBaseDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_base", prop)
  	
  	
//  	//ent.reports.branchs
//    val reportbranchDF = sqlContext.sql("select report_id, branchs from report where branchs is not null")
//    val reportbranchRDD = reportbranchDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("branchs")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//    
//    val reportbranchStructType = reportbranchDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val reportbranchSchema = StructType(StructField("report_id", LongType, false) +: reportbranchStructType.fields)
//    val flatreportbranchDF = sqlContext.createDataFrame(reportbranchRDD, reportbranchSchema)
//    flatreportbranchDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_branch", prop)
//    
//    
//    //ent.reports.changes
//    val reportChangeDF = sqlContext.sql("select report_id, changes from report where changes is not null")
//    val reportChangeRDD = reportChangeDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("changes")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//
//    val reportChangeStructType = reportChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val reportChangeSchema = StructType(StructField("report_id", LongType, false) +: reportChangeStructType.fields)
//    val flatreportChangeDF = sqlContext.createDataFrame(reportChangeRDD, reportChangeSchema)
//    flatreportChangeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_change", prop)
//    
//    
//    //ent.reports.guarantees
//    val reportGuaranteeDF = sqlContext.sql("select report_id, guarantees from report where guarantees is not null")
//    val reportGuaranteeRDD = reportGuaranteeDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("guarantees")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//
//    val reportGuaranteeStructType = reportGuaranteeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val reportGuaranteeSchema = StructType(StructField("report_id", LongType, false) +: reportGuaranteeStructType.fields)
//    val flatreportGuaranteeDF = sqlContext.createDataFrame(reportGuaranteeRDD, reportGuaranteeSchema)
//    flatreportGuaranteeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_guarantee", prop)
//    
//    
//    //ent.reports.inv_ents
//    val invEntsDF = sqlContext.sql("select report_id, inv_ents from report where inv_ents is not null")
//    val invEntsRDD = invEntsDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("inv_ents")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//
//    val invEntsStructType = invEntsDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val invEntsSchema = StructType(StructField("report_id", LongType, false) +: invEntsStructType.fields)
//    val flatinvEntsDF = sqlContext.createDataFrame(invEntsRDD, invEntsSchema)
//    flatinvEntsDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_invest_enterprise", prop)
//    
//    
//    //ent.reports.investment
//    val reportInvestmentDF = sqlContext.sql("select report_id, investment from report where investment is not null")
//    val reportInvestmentRDD = reportInvestmentDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("investment")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//
//    val reportInvestmentStructType = reportInvestmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val reportInvestmentSchema = StructType(StructField("report_id", LongType, false) +: reportInvestmentStructType.fields)
//    val flatreportinvestmentDF = sqlContext.createDataFrame(reportInvestmentRDD, reportInvestmentSchema)
//    flatreportinvestmentDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_stock_investment", prop)
//    
//    
//    //ent.reports.licenses
//    val licenceDF = sqlContext.sql("select report_id, licenses from report where licenses is not null")
//    val licenceRDD = licenceDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("licenses")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//
//    val licenceStructType = licenceDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val licenceSchema = StructType(StructField("report_id", LongType, false) +: licenceStructType.fields)
//    val flatlicenceDF = sqlContext.createDataFrame(licenceRDD, licenceSchema)
//    flatlicenceDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_licence", prop)
//    
//    
//    //ent.reports.operation
//    val operationDF = sqlContext.sql("select report_id, operation from report where operation is not null")
//    val operationRDD = operationDF.rdd.map{
//      x => {
//        val row = x.getAs[Row]("operation")
//        val reportId: Long = x.getAs[Long]("report_id")
//        val cols = row.toSeq
//        val newRow = Row.fromSeq(reportId+:cols)
//        newRow
//      }
//    }
//
//    val operationStructType = operationDF.schema.fields(1).dataType.asInstanceOf[StructType]
//    val operationSchema = StructType(StructField("report_id", LongType, false) +: operationStructType.fields)
//    val flatoperationDF = sqlContext.createDataFrame(operationRDD, operationSchema).withColumnRenamed("fund_subsidy", "gover_subsidy")
//    flatoperationDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_operation", prop)
//    
//    
//    //ent.reports.stock_changes
//    val stockChangeDF = sqlContext.sql("select report_id, stock_changes from report where stock_changes is not null")
//    val stockChangeRDD = stockChangeDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("stock_changes")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//    
//    val stockChangeStructType = stockChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val stockChangeSchema = StructType(StructField("report_id", LongType, false) +: stockChangeStructType.fields)
//    val flatstockChangeDF = sqlContext.createDataFrame(stockChangeRDD, stockChangeSchema)
//    flatstockChangeDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_stock_change", prop)
//    
//    
//    //ent.reports.websites
//    val websiteDF = sqlContext.sql("select report_id, websites from report where websites is not null")
//    val websiteRDD = websiteDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("websites")
//        val reportId: Long = x.getAs[Long]("report_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(reportId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }
//
//    val websiteStructType = websiteDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val websiteSchema = StructType(StructField("report_id", LongType, false) +: websiteStructType.fields)
//    val flatwebsiteDF = sqlContext.createDataFrame(websiteRDD, websiteSchema).withColumnRenamed("type", "type_name")
//    flatwebsiteDF.write.mode(SaveMode.Ignore).jdbc(dbUrl, dstName+"_company_report_website", prop)
    
    reportEntityDF.unpersist()
  }
}
