package com.proud.dc.car

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.data.HDFSUtil
import com.proud.ark.db.ProdIntranetDBUtil
/**
 * spark-shell --master spark://bigdata01:7077 --executor-memory 10g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
 * nohup spark-submit --class com.proud.dc.car.ZhengliQicheQiyeAndFenlei --master spark://bigdata01:7077 --executor-memory 10g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar &
 */
object ZhengliQicheQiyeAndFenlei {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ZhengliQicheQiyeAndFenlei").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
    val companyDF = ProdIntranetDBUtil.loadDFFromTable("dc_import.company", 40, spark).select("id", "name", "address", "scope", "type").filter(x => {
      val typeName = x.getAs[String]("type")
      val scope = x.getAs[String]("scope")
      (typeName == null || !typeName.startsWith("个体")) && (scope != null && scope.contains("汽车"))
    }).select("id", "name", "address", "scope");
    val classifyDF = ProdIntranetDBUtil.loadDFFromTable("test.company_classify", 40, spark)
    val result = companyDF.join(classifyDF, "id");
    ProdIntranetDBUtil.truncate("test.car_company_classify")
    ProdIntranetDBUtil.saveDFToDB(result, "test.car_company_classify")
  }
}