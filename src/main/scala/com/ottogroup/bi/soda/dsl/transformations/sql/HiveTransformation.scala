package com.ottogroup.bi.soda.dsl.transformations.sql

import com.ottogroup.bi.soda.dsl.Transformation
import org.jooq.DSLContext
import org.jooq.Query
import com.ottogroup.bi.soda.dsl.View
import scala.collection.mutable.HashMap
import com.ottogroup.bi.soda.bottler.api.Settings
import scala.util.matching.Regex
import java.io.InputStream
import java.io.FileInputStream
import java.security.MessageDigest
import scala.collection.mutable.HashSet
import org.apache.hadoop.hive.metastore.api.ResourceUri
import org.apache.hadoop.hive.metastore.api.ResourceType
import org.apache.hadoop.hive.metastore.api.Function
import scala.collection.mutable.ListBuffer
import collection.JavaConversions._
import org.apache.commons.io.FilenameUtils

case class HiveTransformation(sql: String*) extends Transformation {
  val functionDefs = ListBuffer[Function]()
  val md5 = MessageDigest.getInstance("MD5")
  def digest(string: String): String = md5.digest(string.toCharArray().map(_.toByte)).map("%02X" format _).mkString
  override def versionDigest=digest(sql.foldLeft(new StringBuilder())((a,b) => a.append(b)).toString)
}

object HiveTransformation extends Transformation {
  
  def apply(f: List[Function], sql: String) = {
    val ht = new HiveTransformation(sql)
    ht.functionDefs ++= f
    ht
  }     
  
  def settingStatements(settings: Map[String, String] = Map()) = {
    val settingsStatements = new StringBuffer()

    for ((key, value) <- settings)
      settingsStatements.append(s"SET ${key}=${value};\n")

    settingsStatements.toString()
  }
  
  def withFunctions(v: View, functions: Map[String,Class[_]] = Map()) = {
    val functionBuff = ListBuffer[Function]()
    
    for ((name,cls) <- functions) {
      val jarName = FilenameUtils.getName(cls.getProtectionDomain.getCodeSource.getLocation.getFile)
      val jarResource = new ResourceUri(ResourceType.JAR, Settings().getDriverSettings(this).location + jarName)
      functionBuff.append(new Function(name, v.dbName, cls.getCanonicalName, null, null, 0, null, List(jarResource)))
    }
    
    functionBuff.toList
  }

  def insertStatement(view: View) = {
    val insertStatement = new StringBuffer()

    insertStatement
      .append("INSERT OVERWRITE TABLE ")
      .append(view.dbName)
      .append(".")
      .append(view.n)

    insertStatement.toString()
  }

  def insertInto(view: View, selectStatement: String, partition: Boolean = true, settings: Map[String, String] = Map(), functions: Map[String,Class[_]] = Map()) = {
    val queryPrelude = new StringBuffer()
    
    queryPrelude
      .append(settingStatements(settings))
      .append(insertStatement(view))

    if (partition && view.partitionParameters.nonEmpty) {
      queryPrelude.append("\nPARTITION (")
      queryPrelude.append(view.partitionParameters.tail.foldLeft({ val first = view.partitionParameters.head; first.n + " = '" + first.v.get + "'" }) { (current, parameter) => current + ", " + parameter.n + " = '" + parameter.v.get + "'" })
      queryPrelude.append(")")
    }

    queryPrelude
    .append("\n")
    .append(selectStatement).toString()
  }

  def insertDynamicallyInto(view: View, selectStatement: String, settings: Map[String, String] = Map()) = {
    val queryPrelude = new StringBuffer()

    val augmentedSettings = new HashMap[String, String]() ++ settings
    augmentedSettings("hive.exec.dynamic.partition") = "true";
    augmentedSettings("hive.exec.dynamic.partition.mode") = "nonstrict";

    queryPrelude
      .append(settingStatements(augmentedSettings.toMap))
      .append(insertStatement(view))

    if (view.partitionParameters.nonEmpty) {
      queryPrelude.append("\nPARTITION (")
      queryPrelude.append(view.partitionParameters.tail.foldLeft(view.partitionParameters.head.n) { (current, parameter) => current + ", " + parameter.n })
      queryPrelude.append(")")
    }

    queryPrelude
      .append("\n")
      .append(selectStatement).toString()
  }

  def replaceParameters(selectStatement: String, parameters: Map[String, Any]): String = {
    if (parameters.isEmpty)
      selectStatement
    else {
      val (key, value) = parameters.head
      val replacedStatement = selectStatement.replaceAll(java.util.regex.Pattern.quote("${" + key + "}"), value.toString().replaceAll("\\$", "|"))
      replaceParameters(replacedStatement, parameters.tail)
    }
  }

  def queryFrom(inputStream: InputStream): String = io.Source.fromInputStream(inputStream, "UTF-8").mkString

  def queryFromResource(resourcePath: String): String = queryFrom(getClass().getClassLoader().getResourceAsStream(resourcePath))

  def queryFrom(filePath: String): String = queryFrom(new FileInputStream(filePath))
}