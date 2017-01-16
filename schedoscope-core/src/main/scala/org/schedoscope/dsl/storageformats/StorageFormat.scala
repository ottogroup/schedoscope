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
package org.schedoscope.dsl.storageformats

/**
  * Base class for all storage formats supported in views
  */
abstract sealed class StorageFormat

/**
  * Store a view's data as Parquet
  *
  * @param fullRowFormatCreateTblStmt instead of using STORED AS PARQUET,
  *                                   expands CREATE TABLE with hive
  *                                   with:
  *                                   ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  *                                   STORED AS
  *                                             INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  *                                             OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  */
case class Parquet(fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat

/**
  * Store a view's data as Avro
  *
  * @param schemaPath                 storage location of Avro schema
  * @param fullRowFormatCreateTblStmt for hive versions prior to 0.13,
  *                                   instead of using STORED AS AVRO,
  *                                   expands CREATE TABLE with hive
  *                                   with:
  *                                   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  *                                   STORED AS
  *                                             INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  *                                             OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  *
  */
case class Avro(schemaPath: String, fullRowFormatCreateTblStmt:Boolean=true) extends StorageFormat

/**
  * Store a view's data as ORC
  *
  * @param fullRowFormatCreateTblStmt for hive versions prior to 0.13,
  *                                   instead of using STORED AS ORC,
  *                                   expands CREATE TABLE with hive
  *                                   with:
  *                                   ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  *                                   STORED AS
  *                                             INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  *                                             OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
  */
case class OptimizedRowColumnar(fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat

/**
  * Store a view's data as RCFile
  *
  * @param fullRowFormatCreateTblStmt for hive versions prior to 0.13,
  *                                   instead of using STORED AS RCFILE,
  *                                   expands CREATE TABLE with hive
  *                                   with:
  *                                   STORED AS
  *                                             INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
  *                                             OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
  */
case class RecordColumnarFile(fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat

/**
  * Store a view's data as SequenceFile
  * @param fieldTerminator          separator for fields, default is \001
  * @param collectionItemTerminator separator for items in collections, default is \002
  * @param mapKeyTerminator         char for separating map keys and values, default is \003
  * @param lineTerminator           default is \n
  * @param fullRowFormatCreateTblStmt for hive versions prior to 0.13,
  *                                   instead of using STORED AS SEQUENCEFILE,
  *                                   expands CREATE TABLE with hive
  *                                   with:
  *                                   STORED AS
  *                                             INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
  *                                             OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
  */
case class SequenceFile(fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null, fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat

/**
  * Store a view's data as a Hive textfile (default)
  *
  * @param fieldTerminator          separator for fields, default is \001
  * @param collectionItemTerminator separator for items in collections, default is \002
  * @param mapKeyTerminator         char for separating map keys and values, default is \003
  * @param lineTerminator           default is \n
  * @param serDe                    custom or native SerDe
  * @param serDeProperties        specify optionally SERDEPROPERTIES
  * @param fullRowFormatCreateTblStmt for hive versions prior to 0.13,
  *                                   instead of using STORED AS TEXTFILE,
  *                                   expands CREATE TABLE with hive
  *                                   with:
  *                                   STORED AS
  *                                             INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  *                                             OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
  */
case class TextFile(fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null, serDe: String = null, serDeProperties: Map[String, String] = null, fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat

/**
  * Convenience case class to store a view's data
  * as TextFile, but automatically setting ROW FORMAT SERDE
  * JsonSerDe
  *
  * @param serDe                  custom SerDe
  * @param serDeProperties        specify optionally SERDEPROPERTIES
  *
  */
case class Json(serDe: String = "org.apache.hive.hcatalog.data.JsonSerDe", serDeProperties: Map[String, String] = null, fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat


/**
  * Convenience case class to store a view's data
  * as TextFile, but automatically setting ROW FORMAT SERDE
  * OpenCSVSerde
  *
  * @param serDe                  custom SerDe
  * @param serDeProperties        specify optionally SERDEPROPERTIES
  * Note: valid for both Comma/Tab Separated (CSV/TSV) formats
  */
case class Csv(serDe: String = "org.apache.hadoop.hive.serde2.OpenCSVSerde", serDeProperties: Map[String, String] = null, fullRowFormatCreateTblStmt:Boolean=false) extends StorageFormat

/**
  * Generic input Output specifier
  * Store a view's data by specifying INPUT and OUTPUT format
  *
  * This case class is only present to allow support any format,
  * it is translated to a STORED AS INPUTFORMAT _ OUTPUTFORMAT _
  * clause
  * In the case of serDe value present, the ROW FORMAT SERDE _
  * is also present
  *
  * Example use case: LZO compression
  *
  * @param input                  custom INPUTFORMAT
  * @param output                 custom OUTPUTFORMAT
  * @param serDe                  custom SerDe
  * @param serDeProperties        specify optionally SERDEPROPERTIES
  */
case class InOutputFormat(input: String, output: String, serDe: String = null, serDeProperties: Map[String, String] = null) extends StorageFormat


/**
  * Convenience case class to store a view's data
  * as any Storage Format, but automatically setting
  * LOCATION 'S3-Bucket'
  *
  * @param bucketName     the unique AWS S3 bucket name
  * @param storageFormat  the type of files to use (e.g. parquet/avro/textFile ...)
  * @param uriScheme      URI scheme associated with hadoop version used, as well AWS region;
  *                       with default value "s3n"; for more information, please check
  *                       https://wiki.apache.org/hadoop/AmazonS3
  */
case class S3(bucketName: String, storageFormat: StorageFormat, uriScheme: String = "s3n") extends StorageFormat