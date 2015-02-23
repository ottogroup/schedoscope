package com.ottogroup.bi.soda.dsl.transformations.filesystem

import com.ottogroup.bi.soda.dsl.Transformation

sealed class FilesystemTransformation extends Transformation
case class CopyFrom(val fromPattern: String, val recursive: Boolean = true) extends FilesystemTransformation
case class Copy(val fromPattern: String, val toPath: String, val recursive: Boolean = true) extends FilesystemTransformation
case class Move(val fromPattern: String, val toPath: String) extends FilesystemTransformation
case class Delete(val fromPattern: String, val recursive: Boolean = false) extends FilesystemTransformation
case class Touch(val fromPath: String) extends FilesystemTransformation
case class IfExists(val path: String, val op: FilesystemTransformation) extends FilesystemTransformation
case class IfNotExists(val path: String, val op: FilesystemTransformation) extends FilesystemTransformation
