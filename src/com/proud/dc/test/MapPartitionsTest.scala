package com.proud.dc.test

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession

object MapPartitionsTest {
  val spark = SparkSession.builder().appName("QiyeGudong").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
  import spark.implicits._
import com.proud.ark.db.DBUtil
case class Gudong(company_name:String, gudong_name:String)
  def main(args: Array[String]): Unit = {
val df = DBUtil.loadDFFromTable("test.gudong_xinxi_sample", 4, spark).select("company_name", "gudong_name").as[Gudong]
val mapDF = df.map(x => x.gudong_name+":"+x.company_name)
val mapPartitionsDF = df.mapPartitions { it => it.map { x => x.gudong_name+":"+x.company_name } }
mapDF.show(100)
mapPartitionsDF.show(100)
  }
}