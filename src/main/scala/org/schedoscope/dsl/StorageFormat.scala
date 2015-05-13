package org.schedoscope.dsl

abstract sealed class StorageFormat

case class TextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat
case class Parquet() extends StorageFormat
case class Avro(schemaPath: String) extends StorageFormat

abstract sealed class ExternalStorageFormat extends StorageFormat
case class ExternalTextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends ExternalStorageFormat
case class ExternalAvro(schemaPath: String) extends ExternalStorageFormat
case class ExaSol(jdbcUrl: String, userName: String, password: String) extends ExternalStorageFormat
case class JDBC(jdbcUrl: String, userName: String, password: String, jdbcDriver: String) extends ExternalStorageFormat
case class Redis(host: String, port: Long = 9393, password: String = "", keys: Seq[String] = List("id"), cols: Seq[Named] = List()) extends ExternalStorageFormat
case class NullStorage extends ExternalStorageFormat