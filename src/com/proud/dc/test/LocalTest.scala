package com.proud.dc.test

import com.proud.dc.util.Util
import com.proud.dc.handle.ZhengliZhaopinXinxi
import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.db.DBUtil
import scala.runtime.ScalaRunTime._
import org.apache.spark.sql.SaveMode

object LocalTest {
  
  val spark = SparkSession.builder().appName("whatever").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master("local[*]").getOrCreate()
  import spark.implicits._
  
  case class Test(name:String, age:Int)
  def main(args: Array[String]): Unit = {
//    val salary = "2万以下/年"
//    val d1 = Util.calc51jobAvgSalary(salary)
//    println(d1)
//    
//    val zhilianSalary = "100万以上/年"
//    val d2 = Util.calcZhilianSalary(zhilianSalary)
//    println(d2)
    
    import org.apache.spark.sql.functions._

    val data = Seq(
      ("michael", 1, "event 1"),
      ("michael", 2, "event 2"),
      ("reynold", 1, "event 3"),
      ("reynold", 3, "event 4")).toDF("user", "time", "event")
    
    DBUtil.saveDFToDB(data, "test.whatever", SaveMode.Overwrite)
  }
}