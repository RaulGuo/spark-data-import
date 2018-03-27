package com.proud.dc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import com.proud.dc.util.ImportUtil
import org.apache.spark.sql.SaveMode

/**
nohup spark-submit --class com.proud.dc.ImportZhuanli --jars /root/data/dependency/mysql-connector-java.jar --conf spark.driver.momory=20G /home/data_center/datacenter-import-0.0.1-SNAPSHOT.jar &
 */

/**
 * 专利信息的导入脚本（主要是一堆column的rename）
 */
object ImportZhuanli {
  
  val spark = SparkSession.builder().appName("DataImport").config("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse").enableHiveSupport().getOrCreate()
  val sc = spark.sparkContext
  
  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc)
    
//    val df = sqlContext.read.json("/home/data_center/source/i.txt")
    val df = sqlContext.read.json("/home/data_center/source/i.txt")
    .withColumnRenamed("专利代理机构", "zhuanye_daili_jigou").withColumnRenamed("专利权人", "zhuanli_quanren").withColumnRenamed("优先权", "youxianquan")
    .withColumnRenamed("代理人", "dailiren").withColumnRenamed("分案原申请", "fenan_yuanshenqing").withColumnRenamed("分类号", "fenleihao").withColumnRenamed("名称", "mingcheng")
    .withColumnRenamed("地址", "dizhi").withColumnRenamed("授权公告号", "shouquan_gonggaohao").withColumnRenamed("授权公告日", "shouquan_gonggaori")
    .withColumnRenamed("摘要", "zhaiyao").withColumnRenamed("申请号", "shenqinghao").withColumnRenamed("申请日", "shenqingri")
    .withColumnRenamed("类型", "leixing").withColumnRenamed("设计人", "shejiren").withColumnRenamed("邮编", "youbian").withColumnRenamed("缩略图", "suolvetu")
    .withColumnRenamed("PCT公布数据", "pct_gongbu_shuju").withColumnRenamed("PCT申请数据", "pct_shenqing_shuju").withColumnRenamed("PCT进入国家阶段日", "pct_guojia_jieduanri")
    .withColumnRenamed("主办单位", "zhuban_danwei").withColumnRenamed("公告号", "gonggaohao").withColumnRenamed("公告日", "gonggaori")
    .withColumnRenamed("发明人", "famingren").withColumnRenamed("同一申请的已公布的文献号", "yigongbu_wenhao")
    .withColumnRenamed("审定公告日", "shending_gonggaori").withColumnRenamed("审定号", "shendinghao")
    .withColumnRenamed("对比文件", "duibi_wenjian").withColumnRenamed("更正文献出版日", "gengzhen_wenxian_chubanri")
    .withColumnRenamed("本国优先权", "benguo_youxianquan").withColumnRenamed("版权所有", "banquan_suoyou")
    .withColumnRenamed("生物保藏", "shengwu_baocang").withColumnRenamed("申请人", "shenqingren")
    .withColumnRenamed("申请公布号", "shenqing_gongbuhao").withColumnRenamed("申请公布日", "shenqing_gongburi")
    .withColumnRenamed("解密公告日", "jiemi_gonggaori")
    .select(
        "zhuanye_daili_jigou", "zhuanli_quanren","youxianquan",
        "dailiren","fenan_yuanshenqing","fenleihao","mingcheng","dizhi",
        "shouquan_gonggaohao","shouquan_gonggaori","zhaiyao","shenqinghao",
        "shenqingri","leixing","shejiren","youbian","suolvetu",
        "pct_gongbu_shuju","pct_shenqing_shuju","pct_guojia_jieduanri","zhuban_danwei","gonggaohao","gonggaori",
        "famingren","yigongbu_wenhao","shending_gonggaori","shendinghao","duibi_wenjian","gengzhen_wenxian_chubanri",
        "benguo_youxianquan","banquan_suoyou","shengwu_baocang","shenqingren","shenqing_gongbuhao","shenqing_gongburi",
        "jiemi_gonggaori")
    
    df.printSchema()
    
//    val sample = sqlContext.read.json("/home/data_center/source/zhuanli.txt")
//    sample.printSchema
    
    df.write.mode(SaveMode.Overwrite).jdbc(ImportUtil.dbUrl, "zhuanli_xinxi_new", ImportUtil.getProperties())
  }
}