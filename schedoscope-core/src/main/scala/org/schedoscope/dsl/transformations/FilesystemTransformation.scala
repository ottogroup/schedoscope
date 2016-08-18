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
package org.schedoscope.dsl.transformations

import java.io.InputStream

import org.schedoscope.dsl.View
import org.schedoscope.scheduler.service.ViewTransformationStatus

/**
  * FileSystem transformations: compute views by copying or moving files
  *
  */
class FilesystemTransformation extends Transformation {
  def name = "filesystem"
}

/**
  * Copy a file from one directory to the view's fullPath
  *
  */
case class CopyFrom(val fromPattern: String, val toView: View, val recursive: Boolean = true) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> CopyFromTransformation",
    Some(Map(
      "from" -> fromPattern,
      "destinationView" -> toView.urlPath, "recursive" -> recursive.toString())))
}

/**
  * Retrieve contents from a stream and store it on the view's fullPath
  *
  */
case class StoreFrom(val inputStream: InputStream, val toView: View) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> StoreFromTransformation",
    Some(Map("destinationView" -> toView.urlPath)))
}

/**
  * Copy file satisfying fromPattern to toPath
  *
  */
case class Copy(val fromPattern: String, val toPath: String, val recursive: Boolean = true) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> CopyTransformation",
    Some(Map(
      "from" -> fromPattern,
      "destinationPath" -> toPath)))
}

/**
  * Move files satisfying fromPattern to toPath
  *
  */
case class Move(val fromPattern: String, val toPath: String) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> MoveTransformation",
    Some(Map(
      "from" -> fromPattern,
      "destinationPath" -> toPath)))
}

/**
  *
  * Delete files satisfying fromPattern
  *
  */
case class Delete(val fromPattern: String, val recursive: Boolean = false) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> DeleteTransformation",
    Some(Map(
      "from" -> fromPattern,
      "recursive" -> recursive.toString)))
}

/**
  * Touch an empty file
  *
  */
case class Touch(val fromPath: String) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> TouchTransformation",
    Some(Map(
      "from" -> fromPath)))
}

/**
  * Create a directory
  *
  */
case class MkDir(val fromPath: String) extends FilesystemTransformation {
  override def viewTransformationStatus = ViewTransformationStatus(
    "filesystem -> MkDirTransformation",
    Some(Map(
      "from" -> fromPath)))
}

/**
  * Wraps a second transformation which will only be executed if the file specified as
  * path does exist
  */
case class IfExists(val path: String, val op: FilesystemTransformation) extends FilesystemTransformation

/**
  * Wraps a second transformation which will only be executed if the file specified as
  * path does not exist
  *
  */
case class IfNotExists(val path: String, val op: FilesystemTransformation) extends FilesystemTransformation
