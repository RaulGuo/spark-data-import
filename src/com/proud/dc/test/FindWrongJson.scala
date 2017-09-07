package com.proud.dc.test

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import java.io.File
import com.proud.ark.db.DBUtil

object FindWrongJson {
  val spark = SparkSession.builder().appName("QiyeGudong").master("local[*]").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()
  def main(args: Array[String]): Unit = {
//  var file = new File("/home/data_center/gudongResult")
//  val files = file.listFiles()
//  files.foreach {x => {
//  val name = x.getName
//  if(name.startsWith("part")){
//    val path = x.getAbsolutePath
//    println(s"-------------${path}--------------")
//    val df = spark.read.json(path)
//    //df.map(x => "insert into company_stock_relationship(company_id, relationship) values ("+x.getAs[Long]("company_id")+", '"+x.getAs[String]("relationship")+"');")
//    df.write.text(path)
//    DBUtil.saveDFToDB(df, "test.company_stock_relationship")
//  }
//  }
//}
	  val df = DBUtil.loadDFFromTable("test.gudong_wrong", spark).select("gudong_name")
	  val sql = "insert into test.gudong_wrong_name(gudong_name) values (?);";
	  df.show()
//    df.foreachPartition { it => {
//    	conn.setAutoCommit(false)
//    	val stmt = conn.prepareStatement(sql)
//    	while(it.hasNext){
//    	  val row = it.next()
//    	  stmt.setString(1, row.getString(0))
//    	  stmt.addBatch()
//    	}
//    	stmt.executeBatch()
//    	stmt.close()
//    	conn.commit()
    	
//      it.foreach { x => {
//        println(x.getString(0)) 
//        val str = x.getString(0)
//        stmt.setString(2, "hello")
//        stmt.executeUpdate()
//      }}
//    }}
    DBUtil.saveDFToDB(df, "test.gudong_wrong_name")
  }
}