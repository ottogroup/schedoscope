/**
  * Copyright 2015 Otto (GmbH & Co KG)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.schedoscope.schema.ddl

import org.schedoscope.dsl.storageformats._
import org.schedoscope.dsl.transformations.Checksum
import org.schedoscope.dsl.{FieldLike, Structure, View}
import scala.collection.mutable.HashMap
import scala.util.matching.Regex

/**
  * Functions for creating Hive CREATE TABLE DDL statements for views
  */
object HiveQl {
  def typeDdl[T](scalaType: Manifest[T]): String = {
    if (scalaType.runtimeClass == classOf[List[_]])
      s"ARRAY<${typeDdl(scalaType.typeArguments(0))}>"
    else if (scalaType.runtimeClass == classOf[Map[_, _]])
      s"MAP<${typeDdl(scalaType.typeArguments(0))},${typeDdl(scalaType.typeArguments(1))}>"
    else if (classOf[Structure].isAssignableFrom(scalaType.runtimeClass)) {
      val s = scalaType.runtimeClass.newInstance().asInstanceOf[Structure]
      s"STRUCT<${fieldsDdl(s)}>"
    } else if (scalaType == manifest[Int])
      "INT"
    else if (scalaType == manifest[Long])
      "BIGINT"
    else if (scalaType == manifest[Byte])
      "TINYINT"
    else if (scalaType == manifest[Boolean])
      "BOOLEAN"
    else if (scalaType == manifest[Double])
      "DOUBLE"
    else if (scalaType == manifest[Float])
      "FLOAT"
    else
      "STRING"
  }

  def commentDdl(view: View): String = view.comment match {
    case Some(c) => s"COMMENT '${c}'"
    case None => ""
  }

  def fieldsDdl(structure: Structure): String = structure
    .fields
    .map { f => s"${f.n}:${typeDdl(f.t)}" }
    .mkString(",\n\t\t")

  def fieldsDdl(view: View): String = view
    .fields
    .map { f => s"${f.n} ${typeDdl(f.t)}" }
    .mkString(",\n\t\t")

  def partitionDdl(view: View): String = {
    val partitioningFields = view
      .partitionParameters
      .map { f => s"${f.n} ${typeDdl(f.t)}" }

    if (!partitioningFields.isEmpty)
      "PARTITIONED BY (" + partitioningFields.mkString(", ") + ")"
    else
      ""
  }

  private def mapToString(m: Map[String, String]) = {
    val result = m.foldLeft("") { (s: String, pair: (String, String)) =>
      s + "\n\t\t '" + pair._1 + "'" + " = " + "'" + pair._2 + "',"
    }
    if (result.length > 0)
      result.dropRight(1)
    else
      result
  }

  def serDePropertiesDdl(serDeProperties: Map[String, String] = null) =
    if (serDeProperties != null)
      "\n\tWITH SERDEPROPERTIES (\n" + mapToString(serDeProperties) + "\n\t)\n"
    else
      ""

  def rowFormatSerDeDdl(serDe: String = null, serDeProperties: Map[String, String] = null) =
    if (serDe != null)
      s"ROW FORMAT SERDE '${serDe}'" + serDePropertiesDdl(serDeProperties)
    else
      ""

  def rowFormatDelimitedDdl(fieldTerminator: String = null,
                            collectionItemTerminator: String = null,
                            mapKeyTerminator: String = null,
                            lineTerminator: String = null) = {
    s"""${if ((fieldTerminator != null) || (collectionItemTerminator != null) || (mapKeyTerminator != null) || (lineTerminator != null)) "ROW FORMAT DELIMITED" else ""}
${if (fieldTerminator != null) s"\tFIELDS TERMINATED BY '${fieldTerminator}'" else ""}
${if (lineTerminator != null) s"\tLINES TERMINATED BY '${lineTerminator}'" else ""}
${if (collectionItemTerminator != null) s"\tCOLLECTION ITEMS TERMINATED BY '${collectionItemTerminator}'" else ""}
${if (mapKeyTerminator != null) s"\tMAP KEYS TERMINATED BY '${mapKeyTerminator}'" else ""}
    """
  }

  def inOutputFormatDdl(input: String, output: String) =
    "\n\tSTORED AS" +
      s"\n\t\tINPUTFORMAT '${input}'" +
      s"\n\t\tOUTPUTFORMAT '${output}'"

  def storedAsDdl(view: View) = view.storageFormat match {

    case TextFile(fieldTerminator, collectionItemTerminator, mapKeyTerminator, lineTerminator, serDe, serDeProperties, fullRowFormatCreateTblStmt) =>
      val rfd = rowFormatDelimitedDdl(fieldTerminator, collectionItemTerminator, mapKeyTerminator, lineTerminator)
      val rowFormat = if (rfd.replaceAll("""(?m)\s+$""", "").length > 0) rfd else rowFormatSerDeDdl(serDe, serDeProperties)
      rowFormat +
        s"${
          if (fullRowFormatCreateTblStmt)
            inOutputFormatDdl("org.apache.hadoop.mapred.TextInputFormat",
              "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat")
          else
            "\n\tSTORED AS TEXTFILE"
        }"

    case SequenceFile(fieldTerminator, collectionItemTerminator, mapKeyTerminator, lineTerminator, fullRowFormatCreateTblStmt) =>
      val rfd = rowFormatDelimitedDdl(fieldTerminator, collectionItemTerminator, mapKeyTerminator, lineTerminator)
      val rowFormat = if (rfd.replaceAll("""(?m)\s+$""", "").length > 0) rfd else ""
      rowFormat +
        s"${
          if (fullRowFormatCreateTblStmt)
            inOutputFormatDdl("org.apache.hadoop.mapred.SequenceFileInputFormat",
              "org.apache.hadoop.mapred.SequenceFileOutputFormat")
          else "\n\tSTORED AS SEQUENCEFILE"
        }"

    case Parquet(fullRowFormatCreateTblStmt) =>
      if (fullRowFormatCreateTblStmt)
        rowFormatSerDeDdl("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") +
          inOutputFormatDdl("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
      else
        "\n\tSTORED AS PARQUET"

    case OptimizedRowColumnar(fullRowFormatCreateTblStmt) =>
      if (fullRowFormatCreateTblStmt)
        rowFormatSerDeDdl("org.apache.hadoop.hive.ql.io.orc.OrcSerde") +
          inOutputFormatDdl("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
      else
        "\n\tSTORED AS ORC"

    case RecordColumnarFile(fullRowFormatCreateTblStmt) =>
      if (fullRowFormatCreateTblStmt)
        inOutputFormatDdl("org.apache.hadoop.hive.ql.io.RCFileInputFormat",
          "org.apache.hadoop.hive.ql.io.RCFileOutputFormat")
      else
        "\n\tSTORED AS RCFILE"

    case Json(serDe, serDeProperties, fullRowFormatCreateTblStmt) =>
      rowFormatSerDeDdl(serDe, serDeProperties) +
        s"${
          if (fullRowFormatCreateTblStmt)
            inOutputFormatDdl("org.apache.hadoop.mapred.TextInputFormat",
              "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat")
          else
            "\n\tSTORED AS TEXTFILE"
        }"

    case Csv(serDe, serDeProperties, fullRowFormatCreateTblStmt) =>
      rowFormatSerDeDdl(serDe, serDeProperties) +
        s"${
          if (fullRowFormatCreateTblStmt)
            inOutputFormatDdl("org.apache.hadoop.mapred.TextInputFormat",
              "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat")
          else
            "\n\tSTORED AS TEXTFILE"
        }"

    case Avro(schemaPath, fullRowFormatCreateTblStmt) =>
      if (fullRowFormatCreateTblStmt)
        rowFormatSerDeDdl("org.apache.hadoop.hive.serde2.avro.AvroSerDe",
          Map("avro.schema.url" -> s"${view.avroSchemaPathPrefix}/${schemaPath}")) +
          inOutputFormatDdl("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      else
        "\n\tSTORED AS AVRO"

    case InOutputFormat(input, output, serDe, serDeProperties) =>
      rowFormatSerDeDdl(serDe, serDeProperties) + inOutputFormatDdl(input, output)

    case _ => "\n\tSTORED AS TEXTFILE"

  }


  def tblPropertiesDdl(view: View) =
    if (view.tblProperties.isEmpty)
      ""
    else
      "TBLPROPERTIES (\n" + mapToString(view.tblProperties.toMap) + "\n\t)"

  def locationDdl(view: View): String = view.tablePath match {
    case "" => ""
    case l => s"LOCATION '${l}'"
  }

  def ddl(view: View): String =
    s"""
\tCREATE EXTERNAL TABLE IF NOT EXISTS ${view.tableName} ${if (view.storageFormat.getClass() != classOf[Avro]) "(\n\t\t" + fieldsDdl(view) + "\n\t)" else ""}
\t${commentDdl(view)}
\t${partitionDdl(view)}
\t${storedAsDdl(view)}
\t${tblPropertiesDdl(view)}
\t${locationDdl(view)}
\t
""".replaceAll("(?m)^[ \t]*\r?\n", "")


  def partitionWhereClause(view: View): String = {
    val whereClause = view
      .partitionParameters
      .map { f => {
        if (f.t == manifest[String])
          s"${f.n}='${f.v.get}'"
        else
          s"${f.n}=${f.v.get}"
      }
      }

    if (!whereClause.isEmpty)
      "WHERE " + whereClause.mkString(" AND ")
    else
      ""
  }

  def ddlChecksum(view: View) = Checksum.digest(
    view.storageFormat match {
      case Avro(schemaPath, _) => ddl(view).replaceAll(Regex.quoteReplacement(s"${view.avroSchemaPathPrefix}/${schemaPath}"), "")

      case _ => ddl(view)
    }
  )

  def selectAll(view: View): String = s"SELECT * FROM ${view.tableName} ${partitionWhereClause(view)}"

  def selectAllOrdered(view: View, orderByField: FieldLike[_]) = s"${selectAll(view)} ORDER BY ${orderByField.n} ASC"
}
