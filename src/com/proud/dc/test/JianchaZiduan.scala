package com.proud.dc.test

object JianchaZiduan {
  def main(args: Array[String]): Unit = {
    val provinces = ShengchengSQL.provincesNoShanghai
    //加postcode,website,email,telephone,introduction,city,logo
    val company = "SELECT id, address, check_at, credit_no, postcode, end_at, formation, leg_rep, name, reg_capi, reg_no, reg_org, scope, start_at, state, term_end_at, term_start_at, `type`, md5, website, email, telephone, introduction, province, city, logo FROM ";
    for(i <- 0 until provinces.length){
//      println(company+provinces(i)+"_company limit 1;")
//      println("alter table "+provinces(i)+"_company add postcode varchar(200), add website varchar(200), add email varchar(200), add telephone varchar(100), add introduction TEXT, add city varchar(100), add logo varchar(200);")
    }
    
//    val abnormal = "SELECT id, company_id, add_at, add_cause, dec_org, remove_at, remove_cause, decision_org from "
    val abnormal = "SELECT id, company_id, add_at, add_cause, dec_org, remove_at, remove_cause from "
    for(i <- 0 until provinces.length){
//      println(abnormal+provinces(i)+"_company_abnormal limit 1;")
    }
    
    val branch = "SELECT id, company_id, name, reg_no, reg_org from "
    for(i <- 0 until provinces.length){
//      println(branch+provinces(i)+"_company_branch limit 1;")
    }
    
    val change = "SELECT id, company_id, `after`, `before`, change_at, item	FROM "
    for(i <- 0 until provinces.length){
//      println(change+provinces(i)+"_company_change limit 1;")
    }
    
    
    val entInvestment = "SELECT company_id, act_amount, act_at, act_type, name, sub_amount, sub_at, sub_type, total_act_amount, total_sub_amount, id from "
    for(i <- 0 until provinces.length){
//      println(entInvestment+provinces(i)+"_company_investment limit 1;")
    }
    
    val equityPledge = "SELECT id, company_id, equity_no, name, pledge_at, pledgee, pledgee_no, pledgor, pledgor_no, pledgor_strand, state FROM "
    for(i <- 0 until provinces.length){
//      println(equityPledge+provinces(i)+"_company_equity_pledge limit 1;")
    }
    
    val member = "SELECT id, company_id, name, position from "
    for(i <- 0 until provinces.length){
//      println(member+provinces(i)+"_company_family_member limit 1;")
    }
    
//    val intellectual = "SELECT company_id, kind, name, register_num, pledgee, pledgor, state, term, id FROM "
    val intellectual = "SELECT company_id, kind, name, pledgee, pledgor, state, term, id FROM "
    for(i <- 0 until provinces.length){
//      println(intellectual+provinces(i)+"_company_intellectual limit 1;")
    }
    
    val license = "SELECT id, company_id, content, end_at, name, number, org, start_at, state FROM company_license"
    for(i <- 0 until provinces.length){
      println(license+provinces(i)+"_company_license limit 1;")
    }
    
    val mortgage = "SELECT id, cert_number, cert_type, name, company_id FROM company_mortgage"
    
    val mortRegister = "SELECT id, company_id, mortgagee, amount, secured_debt_term, kind, secured_remark, scope, debt_amount, reg_debt_term, debt_type, register_num, reg_at, reg_org, reg_remark, secure_scope, state FROM company_mortgage_register"
    
    val punishEnt = "SELECT id, company_id, content, dec_at, detail, leg_rep, name, org_name, paper_no, reg_no, type_name, remark FROM company_punish_ent"
    
    val punishIcbc = "SELECT id, company_id, content, dec_at, detail, leg_rep, name, org_name, paper_no, reg_no, type_name, remark FROM company_punish_icbc"
    
    val spotStock = "SELECT id, company_id, check_at, check_org, result, type_name FROM company_spot_check"
    
    val stockChange = "SELECT id, company_id, `after`, `before`, change_at, item FROM company_stock_change"
    
    val stockHolder = "SELECT id, company_id, cert_no, cert_type, name, type_name FROM company_stock_holder"
    
    val stockInvestment = "SELECT id, company_id, act_amount, act_at, act_type, name, sub_amount, sub_at, sub_type, total_act_amount, total_sub_amount FROM company_stock_investment"
    
    val report = "SELECT id, company_id, address, amount, credit_no, email, employ_num, is_invest, is_stock, is_website, leg_rep, name, postcode, reg_no, relationship, state, telephone, report_at, year, md5 FROM company_report_base"
    
    val reportChange = "SELECT id, report_id, `after`, `before`, change_at, item FROM company_report_change"
    
    val reportGuarantee = "SELECT id, report_id, creditor, debt_amount, debt_kind, debt_term, debtor, guar_range, guar_term, guar_type FROM company_report_guarantee"
    
    val reortInvEnt = "SELECT id, report_id, name, reg_no, inv_comp_id FROM company_report_invest_enterprise"
    
    val reportLicense = "SELECT id, report_id, end_at, name FROM company_report_licence"
    
    val reportOperation = "SELECT id, report_id, financial_loan, gover_subsidy, main_income, net_profit, profit, total_asset, total_debt, total_equity, total_tax, total_turnover FROM company_report_operation"
    
    val reportStockChange = "SELECT id, report_id, `after`, `before`, change_at, stockholder FROM company_report_stock_change"
    
    val reportStockInvestment = "SELECT id, report_id, act_amount, act_at, act_type, name, sub_amount, sub_at, sub_type	FROM company_report_stock_investment"
    
    val reportWebsite = "SELECT id, report_id, name, type_name, url FROM company_report_website"
    
    
    
  }
}