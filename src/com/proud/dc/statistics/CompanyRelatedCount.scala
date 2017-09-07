package com.proud.dc.statistics
/**
 * 企业关联对象的数量的统计。
 * 需要统计的项包括：
 * 公司高管：company_family_member
 * 股东及出资信息：company_contribution
 * 分支机构:company_branch
 * 工商变更: company_change
 * 税务信息：shizong_nashuiren_qsr
 * 经营异常：company_abnormal
 * 股权出质:company_equity_pledge
 * 股权变更:company_stock_change
 * 动产抵押：company_mortgage_register
 * 行政处罚：company_punish_icbc
 * 抽查检查：company_spot_check
 * 证书信息：company_license
 * 对外投资：company_investment_enterprise
 * 企业年报：company_report_base
 * 股东：company_stock_holder
 * 
 * 
 * 软件著作权：bid.company_ruanzhu
 * 企业招聘：jobs.company_employment
 * 专利信息：patent.zhuanli_gongsi_xinxi
 * 税务信息：tax.shizong_nashuiren_qsr
 * 文书信息：wenshu.wenshu_company
 * 
 * 
 * 此外，失信人，中标，裁判文书，由搜索出
 * 网站信息 著作权 法院公告 清算 被执行人信息 司法拍卖 商标 暂时不算
 * 
spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
spark-submit --driver-memory 30g --class com.proud.dc.statistics.CompanyRelatedCount --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/datacenter-import-0.0.1-SNAPSHOT.jar

CREATE TABLE `statistics.company_related_count_1` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`company_id` BIGINT(20) NULL DEFAULT NULL,
	`main_mem` BIGINT(20) NULL DEFAULT NULL COMMENT '公司高管',
	`contribution` BIGINT(20) NULL DEFAULT NULL COMMENT '股东及出资信息',
	`branch` BIGINT(20) NULL DEFAULT NULL COMMENT '分支机构',
	`change` BIGINT(20) NULL DEFAULT NULL COMMENT '工商变更',
	`shuiwu` BIGINT(20) NULL DEFAULT NULL COMMENT '税务信息',
	`abnormal` BIGINT(20) NULL DEFAULT NULL COMMENT '经营异常',
	`equity_pledge` BIGINT(20) NULL DEFAULT NULL COMMENT '股权出质',
	`stock_change` BIGINT(20) NULL DEFAULT NULL COMMENT '股权变更',
	`mort_reg` BIGINT(20) NULL DEFAULT NULL COMMENT '动产抵押',
	`punish` BIGINT(20) NULL DEFAULT NULL COMMENT '行政处罚',
	`spot_check` BIGINT(20) NULL DEFAULT NULL COMMENT '抽查检查',
	`license` BIGINT(20) NULL DEFAULT NULL COMMENT '证书信息',
	`investment` BIGINT(20) NULL DEFAULT NULL COMMENT '对外投资',
	`report` BIGINT(20) NULL DEFAULT NULL COMMENT '企业年报',
	`ruanzhu` BIGINT(20) NULL DEFAULT NULL COMMENT '软件著作权',
	`zhaopin` BIGINT(20) NULL DEFAULT NULL COMMENT '招聘',
	`zhuanli` BIGINT(20) NULL DEFAULT NULL COMMENT '专利',
	`gudong` BIGINT(20) NULL DEFAULT NULL COMMENT '股东',
	PRIMARY KEY (`id`),
	INDEX `company_id` (`company_id`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
 */

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.SaveMode
import com.proud.ark.config.DBName

object CompanyRelatedCount {
	val spark = SparkSession.builder().appName("CompanyRelatedCount").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
  def main(args: Array[String]): Unit = {
	  val resultTable = s"${DBName.statistics}.company_related_count"
	  DBUtil.truncate(resultTable)
	  import spark.implicits._
	  val sqlContext = spark.sqlContext
	  import sqlContext.implicits._
	  
    val mainMem = countByCompanyId("dc_import.company_family_member", "main_mem")
    val contribution = countByCompanyId("dc_import.company_contribution", "contribution")
    val branch = countByCompanyId("dc_import.company_branch", "branch")
    val change = countByCompanyId("dc_import.company_change", "change")
    val shuiwu = countByCompanyId("tax.shizong_nashuiren_qsr", "shuiwu")
    val abnormal = countByCompanyId("dc_import.company_abnormal", "abnormal")
    val equityPledge = countByCompanyId("dc_import.company_equity_pledge", "equity_pledge")
    val stockChange = countByCompanyId("dc_import.company_stock_change", "stock_change")
    val mortReg = countByCompanyId("dc_import.company_mortgage_register", "mort_reg")
    val punish = countByCompanyId("dc_import.company_punish_icbc", "punish")
    val spotCheck = countByCompanyId("dc_import.company_spot_check", "spot_check")
    val license = countByCompanyId("dc_import.company_license", "license")
    val investment = countByCompanyId("dc_import.company_investment_enterprise", "investment", "src_id")
    val report = countByCompanyId("dc_import.company_report_base", "report")

    val ruanzhu = countByCompanyId("bid.company_ruanzhu", "ruanzhu")
    val zhaopin = countByCompanyId("jobs.company_employment", "zhaopin")
    val zhuanli = countByCompanyId("patent.zhuanli_quanren", "zhuanli")
    val gudong = countByCompanyId("dc_import.company_stock_holder", "gudong")
    val wenshu = countByCompanyId("wenshu.wenshu_company", "wenshu")
    
    val seq = Seq("company_id")
    val join = "outer"
    val result = mainMem.join(contribution, seq, join).join(branch, seq, join).join(change, seq, join).join(shuiwu, seq, join).join(abnormal, seq, join).join(equityPledge, seq, join).join(stockChange, seq, join).join(mortReg, seq, join).join(punish, seq, join).join(spotCheck, seq, join).join(license, seq, join).join(investment, seq, join).join(report, seq, join).join(zhuanli, seq, join).join(ruanzhu, seq, join).join(zhaopin, seq, join).join(gudong, seq, join).join(wenshu, seq, join).cache()
    
//    result.write.json("/home/data_center/companyRelatedCount")
    //记得设置数据库的interactive_timeout的值，或者先保存结果，再写入数据库
    DBUtil.saveDFToDB(result, resultTable)
  }
  
  def countByCompanyId(table:String, rename:String, companyIdCol:String = "company_id") = {
		val df = DBUtil.loadDFFromTable(table, spark).select(companyIdCol)
    val result = df.groupBy(companyIdCol).count().withColumnRenamed("count", rename).withColumnRenamed(companyIdCol, "company_id")
//    val re = df.rdd.map { x => (x.getAs[Long](companyIdCol), 1) }.reduceByKey(_+_)
    result
  }
}