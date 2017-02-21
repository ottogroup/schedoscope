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

import org.schedoscope.dsl.storageformats.StorageFormat
import org.schedoscope.dsl.transformations.Transformation

/**
  * Wrapper around a view which marks it as external view.
  * It effectively deletes all dependencies and sets the transformation
  * to NoOp.
  *
  * @param view to convert
  */
case class ExternalView(view: View) extends View {

  //mark view as external
  override val isExternal = true

  override def env_=(env: String): Unit = {
    super.env_=(env)
    view.env = env
  }

  storageFormat = view.storageFormat
  additionalStoragePathPrefix = view.additionalStoragePathPrefix
  additionalStoragePathSuffix = view.additionalStoragePathSuffix
  registeredExports = view.registeredExports
  isMaterializeOnce = view.isMaterializeOnce

  override def fields = view.fields

  override def urlPath = view.urlPath

  override def urlPathPrefix = view.urlPathPrefix

  override def lowerCasePackageName = view.lowerCasePackageName

  override def partitionValues(ignoreSuffixPartitions: Boolean = true) = view.partitionValues(ignoreSuffixPartitions)

  override def partitionParameters = view.partitionParameters

  override def n = view.n

  override def nWithoutPartitioningSuffix = view.nWithoutPartitioningSuffix

  override def hasSuffixPartitions = view.hasSuffixPartitions

  override def suffixPartitionParameters = view.suffixPartitionParameters

  override def isPartition(p: Parameter[_]) = parameters.contains(p)

  override def isSuffixPartition(p: Parameter[_]) = view.isSuffixPartition(p)

  override def module = view.module

  override def dbName = view.dbName

  override def tableName = view.tableName

  override def dbPath = view.dbPath

  override def tablePath = view.tablePath

  override def partitionPath = view.partitionPath

  override def fullPath = view.fullPath

  override def avroSchemaPathPrefix = view.avroSchemaPathPrefix

  override def isPartitioned() = view.isPartitioned()

  override def partitionSpec = view.partitionSpec

  override def namingBase = view.namingBase

  override def nameOf[F <: FieldLike[_]](f: F): Option[String] = view.nameOf(f)

  override def asTableSuffix[P <: Parameter[_]](p: P): P = view.asTableSuffix(p: P)

  override def dependsOn[V <: View : Manifest](df: () => V) = {
    throw new IllegalArgumentException("you can't change the dependencies of an external view")
  }

  override def dependsOn[V <: View : Manifest](dsf: () => Seq[V]) {}

  override def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null) = {}

  override def configureTransformation(k: String, v: Any) = {}

  override def transformVia(ft: () => Transformation) = {}

  override def comment = view.comment

  override def exportTo(export: () => Transformation) = {}

  override def configureExport(k: String, v: Any) = {}

  override def ensureRegisteredParameters = view.ensureRegisteredParameters

  override def parameters = view.parameters

  override def materializeOnce = view.materializeOnce

  override def toString(): String = s"external(${super.toString()})"

}