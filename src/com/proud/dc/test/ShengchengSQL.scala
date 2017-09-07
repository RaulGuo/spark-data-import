package com.proud.dc.test

object ShengchengSQL {
	//所有省份
  val provinces = Array("anhui","beijing","fujian","gansu","guangxi","hainan","hebei","heilongjiang","henan","hubei","hunan","jiangsu","jilin","liaoning","ningxia","qinghai","shandong","shanghai","shanxi","tianjin","xinjiang","xizang","yunnan","zongju")
	//除去上海
	val provincesNoShanghai = Array("anhui","beijing","fujian","gansu","guangxi","hainan","hebei","heilongjiang","henan","hubei","hunan","jiangsu","jilin","liaoning","ningxia","qinghai","shandong","shanxi","tianjin","xinjiang","xizang","yunnan","zongju")
	val map = Map("anhui" -> 1, "beijing" -> 2, "fujian" -> 3, "gansu" -> 4, "guangxi" -> 5, "hainan" -> 6, "hebei" -> 7, "heilongjiang" -> 8, "henan" -> 9, "hubei" -> 10, "hunan" -> 11, "jiangsu" -> 12, "jilin" -> 13, "liaoning" -> 14, "ningxia" -> 15, "qinghai" -> 16, "shandong" -> 17, "shanghai" -> 18, "shanxi" -> 19, "tianjin" -> 20, "xinjiang" -> 21, "xizang" -> 22, "yunnan" -> 23, "zongju" -> 24)
	val tables = Array("company", "company_abnormal", "company_branch", "company_change", "company_ent_change", "company_equity_pledge", "company_family_member", "company_intellectual", "company_contribution", "company_license", "company_mortgage_collateral", "company_mortgage_debt_secure", "company_mortgage_register", "company_mortgagee", "company_punish_ent", "company_punish_icbc", "company_report_base", "company_report_branch", "company_report_change", "company_report_guarantee", "company_report_invest_enterprise", "company_report_licence", "company_report_operation", "company_report_stock_change", "company_report_stock_investment", "company_report_website", "company_spot_check", "company_stock_change", "company_stock_holder", "company_stock_investment")
	//带company_report_base
	val compTables = Array("company", "company_abnormal", "company_branch", "company_change", "company_ent_change", "company_equity_pledge", "company_family_member", "company_intellectual", "company_contribution", "company_license", "company_mortgage_collateral", "company_mortgage_debt_secure", "company_mortgage_register", "company_mortgagee", "company_punish_ent", "company_punish_icbc", "company_spot_check", "company_stock_change", "company_stock_holder", "company_stock_investment","company_report_base")
	//不带company_report_base
  val compTablesWithoutReportBase = Array("company", "company_abnormal", "company_branch", "company_change", "company_ent_change", "company_equity_pledge", "company_family_member", "company_intellectual", "company_contribution", "company_license", "company_mortgage_collateral", "company_mortgage_debt_secure", "company_mortgage_register", "company_mortgagee", "company_punish_ent", "company_punish_icbc", "company_spot_check", "company_stock_change", "company_stock_holder", "company_stock_investment")
	
	val reportTables = Array("company_report_base", "company_report_branch", "company_report_change", "company_report_guarantee", "company_report_invest_enterprise", "company_report_licence", "company_report_operation", "company_report_stock_change", "company_report_stock_investment", "company_report_website")
	
	val remove = Array("beijing_company_ent_change", "beijing_company_mortgage_collateral", "beijing_company_report_branch", "beijing_company_report_stock_change", "fujian_company_contribution", "gansu_company_report_branch", "gansu_company_report_change", "gansu_company_report_licence", "guangxi_company_punish_ent", "guangxi_company_license", "guangxi_company_contribution", "guangxi_company_license", "guangxi_company_punish_ent", "guangxi_company_stock_change", "guangxi_company_stock_holder","guangxi_company_ent_change","hebei_company_contribution", "hunan_company_contribution", "jiangsu_company_ent_change", "jiangsu_company_mortgage_collateral", "jiangsu_company_report_branch", "jiangsu_company_report_change", "jiangsu_company_stock_change", "jilin_company_branch", "jilin_company_mortgage_collateral", "jilin_company_mortgagee", "jilin_company_report_branch", "ningxia_company_report_branch", "shandong_company_mortgage_collateral", "shandong_company_mortgagee", "shandong_company_report_branch", "tianjin_company_ent_change", "tianjin_company_contribution", "tianjin_company_license", "tianjin_company_punish_ent", "xinjiang_company_equity_pledge", "xinjiang_company_punish_ent", "yunnan_company_contribution", "zongju_company_intellectual", "zongju_company_contribution", "zongju_company_mortgage_collateral", "zongju_company_mortgage_debt_secure", "zongju_company_mortgage_register", "zongju_company_mortgagee", "zongju_company_punish_icbc", "zongju_company_report_branch", "zongju_company_report_licence")
	
  def main(args: Array[String]): Unit = {
    
    
    val compDiff = 20000000
    val reportDiff = 15000000

    //ALTER TABLE anhui_company_abnormal ENGINE=MyISAM;
//    for(i <- 0 until provinces.length){
//      for(j <- 0 until tables.length){
//        if(!remove.contains(provinces(i)+"_"+tables(j)))
//          println("alter table "+provinces(i)+"_"+tables(j)+" ENGINE=MyISAM;")
//      }
//    }
    
    //ALTER TABLE `beijing_company_abnormal` ADD COLUMN `province` TINYINT NOT NULL DEFAULT '2', index(province);
//    for(i <- 0 until provinces.length){
//      for(j <- 0 until tables.length){
//        if(!remove.contains(provinces(i)+"_"+tables(j)))
//          println("alter table "+provinces(i)+"_"+tables(j)+" add column province TINYINT NOT NULL DEFAULT "+map.get(provinces(i)).getOrElse(0)+", add index province_index(province);")
//      }
//    }
    
    //添加表的ID字段
    //修改公司表的ID
    //alter table shandong_company_abnormal add column id not null auto_increment, index(id);
//    for(i <- 0 until provinces.length){
//      for(j <- 1 until tables.length){
//        if(!remove.contains(provinces(i)+"_"+tables(j)) && tables(j) != "company_report_base")
//          println("alter table "+provinces(i)+"_"+tables(j)+" add column id bigint(20) NOT NULL auto_increment,add index(id);")
//      }
//    }
    
    
    //修改公司表的ID
    //updata shandong_company set id = id + 20000000 
//    for(i <- 1 until provinces.length){
//      println("update "+provinces(i)+"_company set id = id + "+compDiff*i+";")
//      for(j <- 1 until compTables.length){
//        if(!remove.contains(provinces(i)+"_"+compTables(j)))
//          println("update "+provinces(i)+"_"+compTables(j)+" set company_id = company_id + "+compDiff*i+";")
//      }
//    }
    
    
    //修改基础报表的ID
    //updata shandong_company_report_base set id = id + 20000000 
//    for(i <- 1 until provinces.length){
//      println("update "+provinces(i)+"_company_report_base set id = id + "+reportDiff*i+";")
//      for(j <- 1 until reportTables.length){
//        if(!remove.contains(provinces(i)+"_"+reportTables(j)))
//          println("update "+provinces(i)+"_"+reportTables(j)+" set report_id = report_id +"+reportDiff*i+";")
//      }
//    }
    
    
    //添加公司主表（id）和报表主表ID的索引
    //alter table shandong_company add index(id)
//    for(i <- 1 until provinces.length){
//      println("alter table "+provinces(i)+"_company add index(id);")
//      println("alter table "+provinces(i)+"_company_report_base add index(id);")
//    }
    
    //添加表的company_id的索引
    //alter table company_abnormal add index(company_id)
//    for(i <- 0 until provinces.length){
//      for(j <- 1 until compTables.length){
//        if(!remove.contains(provinces(i)+"_"+compTables(j)))
//          println("alter table "+provinces(i)+"_"+compTables(j)+" add index(company_id);")
//      }
//    }
    
    //添加表的report_id的索引
//    for(i <- 0 until provinces.length){
//      for(j <- 1 until reportTables.length){
//        if(!remove.contains(provinces(i)+"_"+reportTables(j)))
//          println("alter table "+provinces(i)+"_"+reportTables(j)+" add index(report_id);")
//      }
//    }
    
    
    //添加company和report表的md5字段
    //alter table anhui_company add column md5 varchar(40) not null default md5(uuid()), index(md5);
    //alter table shandong_company add index(id)
//    for(i <- 1 until provinces.length){
//      println("alter table "+provinces(i)+"_company add column md5 varchar(40), add index(md5);")
//      println("update "+provinces(i)+"_company set md5 = md5(uuid());")
//      println("alter table "+provinces(i)+"_company_report_base add column md5 varchar(40), add index(md5);")
//      println("update "+provinces(i)+"_company_report_base set md5 = md5(uuid());")
//    }
    
    
    //删除province的默认值
    //alter table anhui_company alter column province drop default;
//    for(i <- 0 until provinces.length){
//      for(j <- 1 until tables.length){
//        if(!remove.contains(provinces(i)+"_"+tables(j)) && tables(j) != "company_report_base")
//          println("alter table "+provinces(i)+"_"+tables(j)+" alter column province drop default;")
//      }
//    }
    
    for(i <- 0 until provinces.length){
        if(!remove.contains(provinces(i)+"_"+tables(0)) && tables(0) != "company_report_base")
          println("alter table "+provinces(i)+"_"+tables(0)+" alter column province drop default;")
    }
    
    //
    
    
  }
  
  
  def getColumnsByTable(table:String){
    //获取表的所有字段
    //select column_name from information_schema.`COLUMNS` c where c.TABLE_SCHEMA = 'dc_import' and c.TABLE_NAME = 'anhui_
    val sql = "select column_name from information_schema.`COLUMNS` c where c.TABLE_SCHEMA = 'dc_import' and c.TABLE_NAME = 'anhui_"+table+"'"
    
  }
  
  def executeSQL(sql:String){
    
  }
  
}