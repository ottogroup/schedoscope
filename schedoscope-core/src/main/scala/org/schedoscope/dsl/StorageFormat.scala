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

abstract sealed class StorageFormat

/**
 * Stores data to a textfile
 * @param fieldTerminator separator for fields, default is tabulator
 * @param collectionItemTerminator separator char for items in collection, default is ,
 * @param mapKeyTerminator char for separating key and value, default is =
 * @param lineTerminator newline
 */
case class TextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat

/**
 * Stores the output of this view as Parquet
 *
 */
case class Parquet() extends StorageFormat

/**
 * Stores as Avro
 * @param schemaPath location of avro schema
 *
 */
case class Avro(schemaPath: String) extends StorageFormat

/**
 *  External storage does not exist within the hadoop warehouse but references external tables
 *  in a RDMBS or Redis instead
 */
abstract sealed class ExternalStorageFormat extends StorageFormat
/**
 * ExternalTextFile has the same properties as Textfile but is stored in the local filesystem
 * instead of HDFS
 *
 */
case class ExternalTextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends ExternalStorageFormat
/**
 * ExternalAvro has the same properties as Avro but is stored in the local filesystem
 * instead of HDFS
 *
 */
case class ExternalAvro(schemaPath: String) extends ExternalStorageFormat
/**
 * Exasolution is currently an alias for JDBC
 */
case class ExaSolution(jdbcUrl: String, userName: String, password: String, merge: Boolean = false, mergeKeys: Seq[String] = List("id")) extends ExternalStorageFormat
/**
 * JDBC tables store the view in an external jdbc database
 *
 * @param jdbcUrl
 * @param userName
 * @param password
 * @param jdbcDriver
 *
 */
case class JDBC(jdbcUrl: String, userName: String, password: String, jdbcDriver: String) extends ExternalStorageFormat
/**
 * Writes to Redis
 */
case class Redis(host: String, port: Long = 9393, password: String = "", keys: Seq[String] = List("id"), cols: Seq[Named] = List()) extends ExternalStorageFormat

/**
 *  Does not store anything, this table is just for side-effects
 */
case class NullStorage extends ExternalStorageFormat
