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
package org.schedoscope.dsl

abstract sealed class StorageFormat

case class TextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat
case class Parquet() extends StorageFormat
case class Avro(schemaPath: String) extends StorageFormat

abstract sealed class ExternalStorageFormat extends StorageFormat
case class ExternalTextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends ExternalStorageFormat
case class ExternalAvro(schemaPath: String) extends ExternalStorageFormat
case class ExaSolution(jdbcUrl: String, userName: String, password: String, merge: Boolean = false, mergeKeys: Seq[String] = List("id")) extends ExternalStorageFormat
case class JDBC(jdbcUrl: String, userName: String, password: String, jdbcDriver: String) extends ExternalStorageFormat
case class Redis(host: String, port: Long = 9393, password: String = "", keys: Seq[String] = List("id"), cols: Seq[Named] = List()) extends ExternalStorageFormat
case class NullStorage extends ExternalStorageFormat