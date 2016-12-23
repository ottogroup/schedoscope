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
  */
case class Parquet() extends StorageFormat

/**
  * Store a view's data as Avro
  *
  * @param schemaPath storage location of Avro schema
  *
  */
case class Avro(schemaPath: String) extends StorageFormat

/**
  * Store a view's data as ORC
  */
case class OptimizedRowColumnar() extends StorageFormat

/**
  * Store a view's data as RCFile
  */
case class RecordColumnarFile() extends StorageFormat

/**
  * Store a view's data as SequenceFile
  * @param fieldTerminator          separator for fields, default is \001
  * @param collectionItemTerminator separator for items in collections, default is \002
  * @param mapKeyTerminator         char for separating map keys and values, default is \003
  * @param lineTerminator           default is \n
  */
case class SequenceFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat

/**
  * Store a view's data as a Hive textfile (default)
  *
  * @param fieldTerminator          separator for fields, default is \001
  * @param collectionItemTerminator separator for items in collections, default is \002
  * @param mapKeyTerminator         char for separating map keys and values, default is \003
  * @param lineTerminator           default is \n
  */
case class TextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat

/**
  * Convenience case class to store a view's data
  * as TextFile, but automatically setting ROW FORMAT SERDE
  * RegexSerde and SERDEPROPERTIES
  */
case class TextfileWithRegEx(inputRegex: String) extends StorageFormat

/**
  * Convenience case class to store a view's data
  * as TextFile, but automatically setting ROW FORMAT SERDE
  * JsonSerDe
  */
case class Json() extends StorageFormat


/**
  * Convenience case class to store a view's data
  * as TextFile, but automatically setting ROW FORMAT SERDE
  * OpenCSVSerde
  * Note: valid for both Comma/Tab Separated (CSV/TSV) formats
  */
case class Csv() extends StorageFormat

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
  */
case class InOutputFormat(input: String, output: String, serDe: Option[String] = None) extends StorageFormat


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