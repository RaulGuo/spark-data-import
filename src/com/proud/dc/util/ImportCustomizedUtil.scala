package com.proud.dc.util

object ImportCustomizedUtil {
  def main(args: Array[String]): Unit = {
    var provinces = Array("anhui","beijing","fujian","gansu","guangxi","hainan","hebei","heilongjiang","henan","hubei","hunan","jiangsu","jilin","liaoning","ningxia","qinghai","shandong","shanghai","shanxi","tianjin","xinjiang","xizang","yunnan","zongju")
    var tables = Array("_company", "_company_abnormal", "_company_branch", "_company_change", "_company_ent_change", "_company_equity_pledge", "_company_family_member", "_company_intellectual", "_company_investment", "_company_license", "_company_mortgage_collateral", "_company_mortgage_debt_secure", "_company_mortgage_register", "_company_mortgagee", "_company_punish_ent", "_company_punish_icbc", "_company_report_base", "_company_report_branch", "_company_report_change", "_company_report_guarantee", "_company_report_invest_enterprise", "_company_report_licence", "_company_report_operation", "_company_report_stock_investment", "_company_report_website", "_company_spot_check", "_company_stock_change", "_company_stock_holder", "_company_stock_investment")
    
    for(i <- 0 until provinces.length){
      for(j <- 0 until tables.length){
        println(provinces(i)+tables(j))
      }
    }
    
  }
}