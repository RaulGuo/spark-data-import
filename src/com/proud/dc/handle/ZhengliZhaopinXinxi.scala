package com.proud.dc.handle


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import com.proud.dc.util.Util
import com.proud.ark.config.DBName
import java.util.regex.Pattern
import com.proud.ark.ml.NBModelBuilder
import com.proud.ark.ml.TokenCNUtil
import org.apache.spark.mllib.linalg.Vectors
import com.proud.ark.db.CrawlerIntranetDBUtil
import com.proud.ark.db.ProdIntranetDBUtil
import com.proud.ark.data.HDFSUtil

/**
 * 整理企业的招聘信息
 * 从148中分别获取51job和智联的企业信息以及招聘信息，
 * 并跟本地的公司进行关联，保存到数据库中
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar
nohup spark-submit --master spark://bigdata01:7077 --executor-memory 9G --class com.proud.dc.handle.ZhengliZhaopinXinxi --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar increment &
nohup spark-submit --master spark://bigdata01:7077 --executor-memory 9G --class com.proud.dc.handle.ZhengliZhaopinXinxi --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar increjobs &
 */

object ZhengliZhaopinXinxi {
  def main(args: Array[String]): Unit = {
    val companyInfoTable = "jobs.company_info"
    val jobInfoTable = "jobs.company_employment"
    
    val spark = SparkSession.builder().appName("ZhengliZhaopinXinxi").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    
    val company = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name").withColumnRenamed("id", "company_id").dropDuplicates("name").persist(StorageLevel.MEMORY_ONLY)
    company.createOrReplaceTempView("company")
    
    
    if(args == null || args.isEmpty || "comp".equals(args(0)) || "company".equals(args(0)) || "increment".equals(args(0)) || "increcomp".equals(args(0))){
//      val sourceDF = if(args == null || args.isEmpty || "comp".equals(args(0)) || "company".equals(args(0))) DBUtil.loadCrawlerData("jobs.job_company", 16, spark).select("id", "company_name", "company_size", "content", "source", "company_logo")
//      else DBUtil.loadCrawlerData("(select c.id, company_name, company_size, content, source, company_logo from jobs.job_company c inner join job_company_monitor m on c.id = m.id) as tmp ", spark)
      val sourceDF = if(args == null || args.isEmpty || "comp".equals(args(0)) || "company".equals(args(0))) CrawlerIntranetDBUtil.loadDFFromTable("jobs.job_company", 16, spark).select("id", "company_name", "company_size", "content", "source", "company_logo")
      else CrawlerIntranetDBUtil.loadDFFromTable("(select c.id, company_name, company_size, content, source, company_logo from jobs.job_company c inner join jobs.job_company_monitor m on c.id = m.id) as tmp ", spark)
      
      //企业的介绍信息需要去重
      val jobCompDF = sourceDF.withColumnRenamed("id", "source_id").withColumnRenamed("company_name", "name").withColumnRenamed("company_size", "size").withColumnRenamed("company_logo", "logo").dropDuplicates("name")
      jobCompDF.createOrReplaceTempView("job_comp")
      
      val compDF = spark.sql("select c.company_id, j.source_id, j.name, j.size, j.logo, j.content, j.source from job_comp j inner join company c on j.name = c.name")
      //全量更新时，先清空表，然后直接插入新数据；增量更新时，使用insert ignore
      if(args == null || args.isEmpty || "comp".equals(args(0)) || "company".equals(args(0))){
//    	  DBUtil.truncate(companyInfoTable)
//    	  DBUtil.saveDFToDB(compDF, companyInfoTable)
    	  
    	  ProdIntranetDBUtil.truncate(companyInfoTable)
    	  ProdIntranetDBUtil.saveDFToDB(compDF, companyInfoTable)
      }else{
//        DBUtil.insertIgnoreDFToDB(compDF, companyInfoTable)
        ProdIntranetDBUtil.insertIgnoreDFToDB(compDF, companyInfoTable)
      }
      println("---------------33333333333333333333----------")
    }
    
    //整理51job和智联的招聘信息
    //需要的字段有：职位名称、薪资、工作经验要求、招聘地址、学历要求、发布日期、数据来源
    if(args == null || args.isEmpty || "job".equals(args(0)) || "increment".equals(args(0)) || "increjobs".equals(args(0))){
      
      val calcSalaryCode:((String, String) => Double) = Util.calcSalary
      val calcSalary = udf(calcSalaryCode)
      
      val calcDateCoder:(String => String) = Util.calcDate
      val calcDateUdf = udf(calcDateCoder)
      
    
      //增量修改必须在参数中指定
//      val sourceDF = if(args == null || args.isEmpty || "job".equals(args(0))) DBUtil.loadCrawlerData("jobs.job_info", 16, spark).select("id", "title", "company_name", "experience", "time", "hangye", "city", "salary", "xueli", "source")
//                      else DBUtil.loadCrawlerData("(select j.id, title, company_name, experience, time, hangye, city, salary, xueli, source from jobs.job_info j inner join jobs.job_info_monitor m on j.id = m.id) as tmp", spark)
      
      val sourceDFWithName = if(args == null || args.isEmpty || "job".equals(args(0))) CrawlerIntranetDBUtil.loadDFFromTable("jobs.job_info", 16, spark).select("id", "title", "company_name", "experience", "time", "hangye", "city", "salary", "xueli", "source")
                      else CrawlerIntranetDBUtil.loadDFFromTable("(select j.id, title, company_name, experience, time, hangye, city, salary, xueli, source from jobs.job_info j inner join jobs.job_info_monitor m on j.id = m.id) as tmp", spark)
      
      val sourceDF = sourceDFWithName.join(company, $"name" === $"company_name").drop("name", "company_name")
      //新添加的整理新的job
      val jobInfoDF = sourceDF.withColumn("publish_at", calcDateUdf(col("time"))).withColumnRenamed("id", "source_id")
  
      //职位的分类
      val (idfModel, model) = NBModelBuilder.initNaiveBayesModel(spark)
      val tokenFunction:Function1[String, Array[String]] = TokenCNUtil.token
      import org.apache.spark.sql.functions._
      val tokenUdf = udf(tokenFunction)
      //对招聘的title进行分词
      val jobDFWithToken = jobInfoDF.withColumn("tokens", tokenUdf(col("title")))
      val tf = NBModelBuilder.getHashingTF()
      //将title进行分词并计算词频，分词后的结果字段保存在term_freq字段中
      val jobDFToPred = tf.transform(jobDFWithToken)
      //用训练出的IDFModel对dataframe计算其TF-IDF的值，用于最后的分类
      val jobIDFToPred = idfModel.transform(jobDFToPred)
      val predictFunc:Function1[org.apache.spark.ml.linalg.SparseVector, Double] = (vector:org.apache.spark.ml.linalg.SparseVector) => {
      	val array = vector.toArray
  			val v = Vectors.dense(array)
  			model.predict(v)
      }
      val predictUdf = udf(predictFunc)
      //添加职位分类
      val jobInfoWithFenlei = jobIDFToPred.withColumn("zhiwei_fenlei", predictUdf(col("idf"))).select("company_id", "title", "source_id", "experience", "time", "hangye", "city", "salary", "xueli", "source", "zhiwei_fenlei", "publish_at")
      jobInfoWithFenlei.createOrReplaceTempView("job_info")
      //分类完成后，跟分类的元数据进行关联，得到具体的分类
      //fenlei_num, fenlei
      val fenleiMetadata = ProdIntranetDBUtil.loadDFFromTable("jobs.zhiwei_fenlei_info", spark)
      fenleiMetadata.createOrReplaceTempView("fenlei_meta")
  
//      jobInfoWithFenlei.join(broadcast(fenleiMetadata), $"zhiwei_fenlei" === $"fenlei_num")
      
      val jobDF = spark.sql("select company_id, source_id, publish_at, salary, xueli as education, source, title, hangye, city, experience, fenlei"
                            +" from job_info j inner join fenlei_meta m on j.zhiwei_fenlei = m.fenlei_num").withColumn("salary_num", calcSalary(col("salary"), col("source")))
      
      if(args == null || args.isEmpty || "job".equals(args(0)) ){
//      	DBUtil.truncate(jobInfoTable)
//      	DBUtil.saveDFToDB(jobDF, jobInfoTable)
        
        ProdIntranetDBUtil.truncate(jobInfoTable)
      	ProdIntranetDBUtil.saveDFToDB(jobDF, jobInfoTable)
      }else{
//        DBUtil.insertOnUpdateDFToDB(jobDF, jobInfoTable)
        ProdIntranetDBUtil.insertOnUpdateDFToDB(jobDF, jobInfoTable)
      }
    }
  }
  
  
  val provCapMap = Map("北京" -> "北京",
            "上海" -> "上海",
            "天津" -> "天津",
            "重庆" -> "重庆",
            "辽宁" -> "沈阳",
            "吉林" -> "长春",
            "黑龙江" -> "哈尔滨",
            "内蒙古" -> "呼和浩特",
            "山西" -> "太原",
            "河北" -> "石家庄",
            "山东" -> "济南",
            "江苏" -> "南京",
            "安徽" -> "合肥",
            "浙江" -> "杭州",
            "福建" -> "福州",
            "广东" -> "广州",
            "广西" -> "南宁",
            "海南" -> "海口",
            "湖北" -> "武汉",
            "湖南" -> "长沙",
            "河南" -> "郑州",
            "江西" -> "南昌",
            "宁夏" -> "银川",
            "新疆" -> "乌鲁木齐",
            "青海" -> "西宁",
            "陕西" -> "西安",
            "甘肃" -> "兰州",
            "四川" -> "成都",
            "云南" -> "昆明",
            "贵州" -> "贵阳",
            "西藏" -> "拉萨")
  
  //用来计算城市，如果location不是异地招聘，就用location。否则根据city的值计算获取省会城市
  def calcCity(city:String, location:String):String = {
    if(location != null && location.trim().length() != 0){
      if(location.trim().equals("异地招聘")){
        if(city != null && city.trim().length() != 0){
          val trimCity = city.trim()
          provCapMap.get(trimCity.replace("省", "")) match {
            case Some(s) => s
            case None => trimCity
          }
        }else{
          null
        }
      }else{
        location
      }
    }else{
      if(city != null && city.trim().length() != 0){
        val trimCity = city.trim()
        provCapMap.get(trimCity.replace("省", "")) match {
          case Some(s) => s
          case None => trimCity
        }
      }else{
        null
      }
    }
  }
  
  val calc51jobCity:((String, String) => String) = calcCity
  val calcCityUdf = udf(calc51jobCity)
  
}