package com.ottogroup.bi.soda.bottler



import java.util.Calendar
import java.sql.DriverManager
import java.sql.Connection
import java.security.MessageDigest
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List
 

object MetastoreTest {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
   	Class.forName("org.apache.hive.jdbc.HiveDriver")
                                                  //> res0: Class[?0] = class org.apache.hive.jdbc.HiveDriver
   System.getProperty("java.version")             //> res1: String = 1.7.0_55
   
    val connection =DriverManager.getConnection("jdbc:hive2://anatolefrance:10000/default;principal=hive/anatolefrance@OTTOGROUP.COM")
                                                  //> log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics
                                                  //| 2.lib.MutableMetricsFactory).
                                                  //| log4j:WARN Please initialize the log4j system properly.
                                                  //| log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for mor
                                                  //| e info.
                                                  //| connection  : java.sql.Connection = org.apache.hive.jdbc.HiveConnection@712e
                                                  //| afbf
    val stmt=connection.createStatement()         //> stmt  : java.sql.Statement = org.apache.hive.jdbc.HiveStatement@30145ea6
    stmt.execute("DROP TABLE test123")            //> res2: Boolean = false
    val sql= "CREATE external TABLE test123(id INT) LOCATION '/user/dev_hzorn/test123'"
                                                  //> sql  : String = CREATE external TABLE test123(id INT) LOCATION '/user/dev_hz
                                                  //| orn/test123'
    val md5 = MessageDigest.getInstance("MD5").digest(sql.toCharArray().map(_.toByte)).map("%02X" format _).mkString
                                                  //> md5  : String = ECD4772C5587943FB1ACBDC24359E5F5
    val ret=stmt.execute(sql)                     //> ret  : Boolean = false
    val ret2 = stmt.execute(s"ALTER TABLE test123 SET TBLPROPERTIES ('hash'='${md5}')")
                                                  //> ret2  : Boolean = false
    val info = stmt.execute("describe extended test123")
                                                  //> info  : Boolean = true
 		val rs=stmt.getResultSet()        //> rs  : java.sql.ResultSet = org.apache.hive.jdbc.HiveQueryResultSet@2a35606a
                                                  //| 
 		while(rs.next()         ) {
 		val info2=rs.getMetaData()
    for ( i <- 1 to info2.getColumnCount()) {
      if (rs.getString(1)=="Detailed Table Information") {
   		 	val  tableInformation=rs.getString(2)
   		 	}
    	}
    	 	}
    connection.close();
}