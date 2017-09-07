package com.proud.dc.test

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil

/**
 * 测试减少shuffle的方法
 */

object JianshaoShuffleTest {
  val spark = SparkSession.builder().appName("QiyeGudong").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
  def main(args: Array[String]): Unit = {
    val rdd = spark.sparkContext.textFile("/home/data_center/nohup.out").map { x => x.substring(0, 5) }.cache()
    
    
    
    //自选方案
    val rdd1 = rdd.map { x => (x.charAt(0), x) }.groupByKey(1000).map[(Char, Int)]{case (c, it) => {
      (c, it.count { x => true })
    }}.collectAsMap()
//    println(rdd1.collect())
    
    //案例中的原始方案：
    val rdd2 = rdd.map { x => (x.charAt(0), x) }.groupByKey().mapValues { x => x.size }.collectAsMap()
//    println(rdd2.collect())
    
    //优化方案：
    val rdd3 = rdd.distinct(numPartitions = 6).map { x => (x.charAt(0), 1) }.reduceByKey(_+_).collectAsMap()
//    println(rdd3.collect())
    
    //最后一个方案：
    val rdd4 = rdd.map { x => (x.charAt(0), x) }.countByKey()
    
    val groupByRdd = rdd.map { x => (x.charAt(0), x) }.groupByKey()
    val aggByRdd = rdd.map{ x => (x.charAt(0), x) }
    
    
    
  }
}