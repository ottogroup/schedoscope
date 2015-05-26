package com.ottogroup.bi.soda.dsl

abstract class StorageFormat

case class TextFile(val fieldTerminator: String = null, collectionItemTerminator: String = null, mapKeyTerminator: String = null, lineTerminator: String = null) extends StorageFormat
case class Parquet() extends StorageFormat
case class Avro(schemaPath: String) extends StorageFormat