package com.proud.dc.handle

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import com.proud.ark.data.HDFSUtil
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType

/**
 spark-submit --master spark://bigdata01:7077 --executor-memory 10G --class com.proud.dc.handle.ChuliGongkongXinxi --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar
 */
object ChuliGongkongXinxi {
  
  case class CompCate(company_id:Long, category:String)
  case class CompanyInfo(category:Array[String], company_name:String, company_type:String, major_prod:String,user_ratings:String, public_praise:String,
      public_praise_compare:String, popularity:String, popularity_compare:String, collection:String, address:String, postcodes:String, telephone:String, 
      fax:String, website:String, email:String, details:String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("GongkongXinxi").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).enableHiveSupport().getOrCreate()
    import spark.implicits._
    
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "company_id")
    import org.apache.spark.sql.functions._
    //对重名企业的处理：
    val gkCompany = DBUtil.loadCrawlerData("(select c.*, d.details from gongkong.t_company c inner join gongkong.t_company_details d using(id)) as tmp", spark).drop("id").groupByKey(x => x.getAs[String]("companyTitle")).mapGroups{
      case(name, it) => {
        val rowSet = it.toSet
        val cateSet = getArrayCol(rowSet, "companyCategory").flatMap { x => x.split(",") }.toArray
        val companyType = getMaxLengthVal(getArrayCol(rowSet, "companyType"))
        val majorProd = getMaxLengthVal(getArrayCol(rowSet, "majorProduct"))
        val userRatings = getMaxLengthVal(getArrayCol(rowSet, "userRatings"))
        val publicPraise = getMaxLengthVal(getArrayCol(rowSet, "publicPraise"))
        val publicPraiseCompare = getMaxLengthVal(getArrayCol(rowSet, "publicPraiseCompare"))
        val popularity = getMaxLengthVal(getArrayCol(rowSet, "popularity"))
        val popularityCompare = getMaxLengthVal(getArrayCol(rowSet, "popularityCompare"))
        val collection = getMaxLengthVal(getArrayCol(rowSet, "collection"))
        val address = getMaxLengthVal(getArrayCol(rowSet, "address"))
        val postcodes = getMaxLengthVal(getArrayCol(rowSet, "postcodes"))
        val telephone = getMaxLengthVal(getArrayCol(rowSet, "telephone"))
        val fax = getMaxLengthVal(getArrayCol(rowSet, "fax"))
        val website = getMaxLengthVal(getArrayCol(rowSet, "website"))
        val email = getMaxLengthVal(getArrayCol(rowSet, "email"))
        val details = getMaxLengthVal(getArrayCol(rowSet, "details"))
        CompanyInfo(cateSet, name, companyType, majorProd, userRatings, publicPraise, publicPraiseCompare, popularity, popularityCompare,
            collection, address, postcodes, telephone, fax, website, email, details)
      }
    }
//    val gkCompDetail = DBUtil.loadCrawlerData("gongkong.t_company_details", spark).withColumnRenamed("id", "detail_id")
    
    //repartition(1)是为了让自增ID是连续的
    val gkCompanyDF = companyDF.join(gkCompany, $"name" === $"company_name").repartition(1).withColumn("id", monotonically_increasing_id+1).persist()
    val cates = gkCompanyDF.select("company_id", "category").flatMap(x => {
      val cates = x.getAs[Seq[String]]("category")
      cates.map(c => CompCate(x.getAs[Long]("company_id"), c.trim()))
    })
    
    DBUtil.truncateIC("gongkong.company_category")
    DBUtil.saveDFToICDB(cates, "gongkong.company_category")
    
    DBUtil.truncateIC("gongkong.gongkong_company_detail")
    DBUtil.saveDFToICDB(gkCompanyDF.select("id", "details"), "gongkong.gongkong_company_detail")

    DBUtil.truncateIC("gongkong.gongkong_company")
    DBUtil.saveDFToICDB(gkCompanyDF.drop("category", "details", "name"), "gongkong.gongkong_company")
    
  }
  
  //从row的集合中提取出某个列
  def getArrayCol(rows:Set[Row], name:String):Set[String] = {
    val result = rows.map { x => x.getAs[String](name) }
    result
  }
  
  //获取某个列中最长的字符串
  def getMaxLengthVal(vals:Set[String]):String = {
    if(vals == null || vals.isEmpty)
      return null;
    vals.reduce[String]{case(v1, v2) => {
      if(v1 == null || v1.isEmpty())
        return v2
      else if(v2 == null || v2.isEmpty())
        return v1
      else if(v1.length() > v2.length())
        return v1
      else
        return v2
    }}
  }
  
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
}