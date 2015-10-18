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

import org.schedoscope.dsl.Structure

import org.schedoscope.dsl.View
import org.schedoscope.dsl.storageformats._

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
    case None    => ""
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

  def storedAsDdl(
    view: View) = view.storageFormat match {
    case Parquet() => "STORED AS PARQUET"

    case Avro(schemaPath) => s"""ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
\tWITH SERDEPROPERTIES (
\t\t'avro.schema.url' = '${view.avroSchemaPathPrefix}/${schemaPath}'
\t)
\tSTORED AS
\t\tINPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
\t\tOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'"""

    case TextFile(fieldTerminator, collectionItemTerminator, mapKeyTerminator, lineTerminator) => s"""${if ((fieldTerminator != null) || (collectionItemTerminator != null) || (mapKeyTerminator != null) || (lineTerminator != null)) "ROW FORMAT DELIMITED" else ""}
${if (fieldTerminator != null) s"\tFIELDS TERMINATED BY \42${fieldTerminator}\42" else ""}
${if (lineTerminator != null) s"\tLINES TERMINATED BY \42${lineTerminator}\42" else ""}
${if (collectionItemTerminator != null) s"\tCOLLECTION ITEMS TERMINATED BY \42${collectionItemTerminator}\42" else ""}
${if (mapKeyTerminator != null) s"\tMAP KEYS TERMINATED BY \42${mapKeyTerminator}\42" else ""}
\tSTORED AS TEXTFILE"""
    case e: ExternalStorageFormat => "STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'"
    case _                        => "STORED AS TEXTFILE"
  }

  def locationDdl(view: View): String = view.locationPath match {
    case "" => ""
    case l  => if (!view.isExternal) s"LOCATION '${l}'" else ""
  }

  def ddl(view: View): String = s"""
\tCREATE EXTERNAL TABLE IF NOT EXISTS ${view.tableName} ${if (view.storageFormat.getClass() != classOf[Avro]) "(\n\t\t" + fieldsDdl(view) + "\n\t)" else ""}
\t${commentDdl(view)}
\t${partitionDdl(view)}
\t${storedAsDdl(view)}
\t${locationDdl(view)}
\t
""".replaceAll("(?m)^[ \t]*\r?\n", "")

  def partitionWhereClause(view: View): String = {
    val whereClause = view
      .partitionParameters
      .map { f =>
        {
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

  def selectAll(view: View): String = s"SELECT * FROM ${view.tableName} ${partitionWhereClause(view)}"

}
