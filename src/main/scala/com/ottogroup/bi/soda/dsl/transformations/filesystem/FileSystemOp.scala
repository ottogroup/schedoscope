package com.ottogroup.bi.soda.dsl.transformations.filesystem

import com.ottogroup.bi.soda.dsl.Transformation

sealed class FileOperation extends Transformation
case class CopyFrom(val fromPattern: String, val recursive: Boolean = true) extends FileOperation
case class Copy(val fromPattern: String, val toPath: String, val recursive: Boolean = true) extends FileOperation
case class Move(val fromPattern: String, val toPath: String) extends FileOperation
case class Delete(val fromPattern: String, val recursive: Boolean = false) extends FileOperation
case class Touch(val fromPath: String) extends FileOperation
case class IfExists(val path: String, val op: FileOperation) extends FileOperation
case class IfNotExists(val path: String, val op: FileOperation) extends FileOperation
