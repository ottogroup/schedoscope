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
  * Store a view's data as a Hive textfile (default)
  *
  * @param fieldTerminator          separator for fields, default is \001
  * @param collectionItemTerminator separator for items in collections, default is \002
  * @param mapKeyTerminator         char for separating map keys and values, default is \003
  * @param lineTerminator           default is \n
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
