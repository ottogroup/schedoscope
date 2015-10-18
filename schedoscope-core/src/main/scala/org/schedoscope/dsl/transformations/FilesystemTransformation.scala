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

import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.View
import java.io.InputStream

/**
 * Class of transformation that operates directly on the filesystem
 *
 */
sealed class FilesystemTransformation extends Transformation {
  override def name = "filesystem"
}
/**
 * Copy a file from one directory to the view location path
 *
 */
case class CopyFrom(val fromPattern: String, val toView: View, val recursive: Boolean = true) extends FilesystemTransformation
/**
 * Retrieve contents of a stream and store it to the view location path
 *
 */
case class StoreFrom(val inputStream: InputStream, val toView: View) extends FilesystemTransformation
/**
 * copy a file from a to b
 *
 */
case class Copy(val fromPattern: String, val toPath: String, val recursive: Boolean = true) extends FilesystemTransformation
/**
 * move files from a to b
 *
 */
case class Move(val fromPattern: String, val toPath: String) extends FilesystemTransformation
/**
 * delete files
 *
 */
case class Delete(val fromPattern: String, val recursive: Boolean = false) extends FilesystemTransformation
/**
 * create empty file
 *
 */
case class Touch(val fromPath: String) extends FilesystemTransformation
/**
 * create a directory
 *
 */
case class MkDir(val fromPath: String) extends FilesystemTransformation
/**
 * Wraps a second transformation which will only be executed if the file specified as
 * path does exist
 *
 */
case class IfExists(val path: String, val op: FilesystemTransformation) extends FilesystemTransformation
/**
 * Wraps a second transformation which will only be executed if the file specified as
 * path does not exist
 *
 */
case class IfNotExists(val path: String, val op: FilesystemTransformation) extends FilesystemTransformation
