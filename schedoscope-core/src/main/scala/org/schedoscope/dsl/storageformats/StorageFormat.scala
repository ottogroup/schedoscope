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

import org.schedoscope.dsl._

/**
 * Base class for all storage formats supported in views
 */
abstract sealed class StorageFormat

/**
 * Store a view's data as a Hive textfile (default)
 *
 * @param fieldTerminator separator for fields, default is \001
 * @param collectionItemTerminator separator for items in collections, default is \002
 * @param mapKeyTerminator char for separating map keys and values, default is \003
 * @param lineTerminator default is \n
 */
case class TextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat

/**
 * Store a view's data as Parquet
 *
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
 * Base class for external storage formats. Views with external storage formats keep their data not within the Hive metastore
 * but in external pool such as in a RDBMS tables or Redis stores
 */
abstract sealed class ExternalStorageFormat extends StorageFormat

/**
 * Store a view's data in an externalTextFile outside of HDFS in the local file system of the schedoscope process.
 *
 * @param fieldTerminator separator for fields, default is \001
 * @param collectionItemTerminator separator for items in collections, default is \002
 * @param mapKeyTerminator char for separating map keys and values, default is \003
 * @param lineTerminator default is \n
 *
 */
case class ExternalTextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends ExternalStorageFormat

/**
 * Store a view's data in an Avro file outside of HDFS in the local file system of the schedoscope process.
 *
 * @param schemaPath storage location of Avro schema
 *
 *
 */
case class ExternalAvro(schemaPath: String) extends ExternalStorageFormat

/**
 * Store a view's data in the ExaSol RDBMS via the Exasol JDBC driver
 *
 * @param jdbcUrl
 * @param userName
 * @param password
 * @param jdbcDriver
 *
 */
case class ExaSolution(jdbcUrl: String, userName: String, password: String, merge: Boolean = false, mergeKeys: Seq[String] = List("id")) extends ExternalStorageFormat

/**
 * Currently, JDBC is just a synonym for ExaSolution. There is no generic JDBC external storage format yet.
 *
 * @param jdbcUrl
 * @param userName
 * @param password
 * @param jdbcDriver
 *
 */
case class JDBC(jdbcUrl: String, userName: String, password: String, jdbcDriver: String) extends ExternalStorageFormat

/**
 * Store a view's data in an external Redis cluster.
 *
 * @param host
 * @param port
 * @param password
 */
case class Redis(host: String, port: Long = 9393, password: String = "", keys: Seq[String] = List("id"), cols: Seq[Named] = List()) extends ExternalStorageFormat

/**
 * Does not store anything, this table is just for side-effects
 */
case class NullStorage() extends ExternalStorageFormat
