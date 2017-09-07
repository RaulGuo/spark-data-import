package com.proud.dc.util

import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.security.MessageDigest
import org.apache.commons.codec.digest.DigestUtils
import java.util.UUID
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.text.ParseException

object Util {
  
  val df = new SimpleDateFormat("yyyy-MM-dd")
  
  def getStartDate(year:Int):Date = {
    var c = Calendar.getInstance
    c.set(year, 1, 1)
    new Date(c.getTimeInMillis)
  }
  
  def getEndDate(year:Int):Date = {
    var c = Calendar.getInstance
    c.set(year, 12, 31)
    new Date(c.getTimeInMillis)
  }
  
  def getYearFromDate(d:Timestamp):Int = {
    val c = Calendar.getInstance
    c.setTimeInMillis(d.getTime)
    c.get(Calendar.YEAR)
  }
  
  def generateMd5():String = {
    val uuid = UUID.randomUUID().toString()
    DigestUtils.md5Hex(uuid)
  }
  
  import scala.util.{Try, Success, Failure}
  def getDateFromString(str:String):String = {
    if(str == null || str.trim().isEmpty())
      return null
    else{
      try{
        val date = df.parse(str)
        return str.trim()
      } catch {
        case e:ParseException => return null
        case _:Exception => return null
      }
    }
  }
  
  val calcDate:Function1[String, String] = getDateFromString
  val genMd5Func:Function0[String] = generateMd5
  val genMd5Udf = udf(genMd5Func)
  /**
   * 对51job的薪资计算：
   * 51job中的薪资的主要格式：
   ** 1-2万/月
   ** 1+ 万/月
   ** 3-4千/月
   ** 2000-2999/月
   ** 3+ 千/月
   ** 3000+ /月
   ** 80000-150000/年
   ** 10-15万/年
   ** 6+ 万/年
   * 100元/小时
   * 150元/天
   * 100万以上/年
   * 10万以上/月
   * 130000+ /年
   * 2万以下/年
   * 100000及以上/月
   */
  def calcSalary(salary:String, source:String):Double = {
    if("智联招聘".equals(source)){
      return calcZhilianSalary(salary).floor
    }else if("51job".equals(source)){
      return calc51jobAvgSalary(salary).floor
    }else {
      return -1
    }
  }
  
  
  def calc51jobAvgSalary(salary:String):Double = {
    
    //时薪和日薪不参与计算
    if(salary == null || salary.trim().length() == 0 || salary.endsWith("/小时") || salary.endsWith("/小时") || salary.endsWith("/天")){
      return -1
    }
    try{
      //"3+ 万/月"或者"3-4万/月"
      if(salary.contains("万/月")){
        if(salary.contains("-")){
          val str = salary.replace("万/月", "").split("-").map { x => x.trim().toDouble }
          return 10000*(str(1)+str(0))/2
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble*10000
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
      //"3+ 千/月"或者"3-4千/月"
      if(salary.endsWith("千/月")){
        if(salary.contains("-")){
          val str = salary.replace("千/月", "").split("-").map { x => x.trim().toDouble }
          return 1000*(str(1)+str(0))/2
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble*1000
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
      if(salary.endsWith("千以下/月")){
        return salary.replace("千以下/月", "").toDouble*1000
      }
           
      if(salary.endsWith("以下/月")){
        return salary.replace("以下/月", "").toDouble
      }
      
      
      //万以上/月
      if(salary.endsWith("万以上/月")){
        return salary.replace("万以上/月", "").toDouble*10000
      }
      
      //及以上/月
      if(salary.endsWith("及以上/月")){
        return salary.replace("及以上/月", "").toDouble
      }
      
      //"2000-2999/月"或者"3000+ /月"
      if(salary.contains("/月")){
        if(salary.contains("-")){
          val str = salary.replace("/月", "").split("-").map { x => x.trim().toDouble }
          return (str(1)+str(0))/2
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
      if(salary.endsWith("千/年")){
        if(salary.contains("-")){
          val str = salary.replace("千/年", "").split("-").map { x => x.trim().toDouble }
          return 1000*(str(1)+str(0))/24
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble*1000/12
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
      //"6+ 万/年"或者"10-15万/年"
      if(salary.contains("万/年")){
        if(salary.contains("-")){
          val str = salary.replace("万/年", "").split("-").map { x => x.trim().toDouble }
          return 10000*(str(1)+str(0))/12
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble*10000/12
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
      if(salary.endsWith("万以下/年")){
        return salary.replace("万以下/年", "").toDouble*10000/12
      }
      
      //万以下/年
      if(salary.endsWith("万以上/年")){
        return salary.replace("万以上/年", "").toDouble*10000/12
      }
      
      
      //"80000-150000/年"
      if(salary.contains("/年")){
        if(salary.contains("-")){
          val str = salary.replace("/年", "").split("-").map { x => x.trim().toDouble }
          return (str(1)+str(0))/12
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble/12
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
//      if(salary.endsWith("元/小时")){
//        return salary.replace("元/小时", "").toDouble*8*22
//      }
//      
//      if(salary.endsWith("元/天")){
//        return salary.replace("元/天", "").toDouble*22
//      }
 
      
      if(salary.endsWith("...")){
        val sals = salary.replace("...", "").split("-").map { x => x.trim().toDouble }
        return (sals(1)+sals(0))/12
      }
      println("--------------new form of salary:"+salary)
      return -1
    } catch {
      case _:Throwable => {
        println("--------------new form of salary:"+salary)
        return -1
      }
    }
  }
  
  def test(salary:String, data:Int):Double = {
    0
  }
  
  def calcZhilianSalary(salary:String):Double = {
    if(salary == null || salary.trim.length() == 0 || salary.trim().equals("面议") || salary.endsWith("/小时") || salary.endsWith("/时") || salary.endsWith("/天")){
      return -1
    }
    
    try{
      if(salary.contains("-")){
        val sals = salary.replace("元/月 ", "").split("-").map { x => x.trim.toDouble }
        return (sals(0)+sals(1))/2
      }
      
      if(salary.contains("~")){
        val sals = salary.replace("元/月 ", "").split("~").map { x => x.trim.toDouble }
        return (sals(0)+sals(1))/2
      }
      
      if(salary.contains("元以")){
        return salary.substring(0, salary.indexOf("元以")).trim().toDouble
      }
      
      if(salary.endsWith("千以下/月")){
        return salary.replace("千以下/月", "").toDouble*1000
      }
      
      if(salary.contains("千/年")){
        val str = salary.replace("千/年", "").split("-").map { x => x.trim().toDouble }
        return 1000*(str(1)+str(0))/24
      }
      
      if(salary.endsWith("以下/月")){
        return salary.replace("以下/月", "").toDouble
      }
      
      if(salary.endsWith("万以下/年")){
        return salary.replace("万以下/年", "").toDouble*10000/12
      }
      
      if(salary.endsWith("万以上/年")){
        return salary.replace("万以上/年", "").toDouble*10000/12
      }
      
      if(salary.endsWith("...")){
        val sals = salary.replace("...", "").split("-").map { x => x.trim().toDouble }
        return (sals(1)+sals(0))/12
      }
      
      //"80000-150000/年"或者"200000+ /年"
      if(salary.contains("/年")){
        if(salary.contains("-")){
          val str = salary.replace("/年", "").split("-").map { x => x.trim().toDouble }
          return (str(1)+str(0))/12
        }else if(salary.contains("+")){
          return salary.substring(0, salary.indexOf("+")).trim().toDouble/12
        }else{
          println("--------------new form of salary:"+salary)
          return -1
        }
      }
      
      println("--------------new form of salary:"+salary)
      return -1
    }catch {
      case _:Throwable => {
        println("--------------new form of salary:"+salary)
        return -1
      }
    }
    
  }
}