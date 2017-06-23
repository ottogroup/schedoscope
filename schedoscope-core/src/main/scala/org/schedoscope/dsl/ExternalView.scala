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
import org.schedoscope.lineage.DependencyMap

import scala.util.Try

/**
  * Wrapper around a view which marks it as external view.
  * It effectively deletes all dependencies and sets the transformation
  * to NoOp.
  */
case class ExternalView(view: View) extends View {

  //
  //mark view as external
  //

  override val isExternal = true

  //
  // Give us a string representation clearly showing this is an external view
  //
  override def toString(): String = s"external(${super.toString()})"

  //
  // Stop dependency hierarchy now and here
  //
  override def dependencies: List[View] = List()

  //
  // Shunt View DSL view modification methods.
  //

  override def external(view: View): ExternalView = {
    throw new IllegalArgumentException("You can't change a dependency to external on an external view")
  }

  override def dependsOn[V <: View : Manifest](dsf: () => Seq[V]): Unit = {
    throw new IllegalArgumentException("You can't change the dependencies of an external view")
  }

  override def transformVia(ft: () => Transformation): Unit = {
    throw new IllegalArgumentException("You can't change the transformation of an external view")
  }

  override def configureTransformation(k: String, v: Any): Unit = {
    throw new IllegalArgumentException("You can't configure the transformation of an external view")
  }

  override def dependsOn[V <: View : Manifest](df: () => V): () => V = {
    throw new IllegalArgumentException("You can't change the dependencies of an external view")
  }

  override def exportTo(ft: () => Transformation): Unit = {
    throw new IllegalArgumentException("You cannot add an export to an external view")
  }

  override def muteExports(): Unit = {
    throw new IllegalArgumentException("You cannot mute exports of an external view")
  }

  override def configureExport(k: String, v: Any): Unit = {
    throw new IllegalArgumentException("You cannot reconfigure exports of an external view")
  }

  override def storedAs(f: StorageFormat, additionalStoragePathPrefix: String, additionalStoragePathSuffix: String): Unit = {}

  override def tblProperties(m: Map[String, String]): Unit = {
    throw new IllegalArgumentException("You cannot change table properties of an external view")
  }

  override def materializeOnce: Unit = {
    throw new IllegalArgumentException("You cannot change materialization of an external view")
  }

  override def registerField(f: Field[_]): Unit = {
    throw new IllegalArgumentException("You cannot register fields with an external view")
  }

  override def privacySensitive[P <: FieldLike[_]](ps: P): P = {
    throw new IllegalArgumentException("You cannot set fields as privacy-related with an external view")
  }

  override def comment(aComment: String): Unit = {
    throw new IllegalArgumentException("You cannot set comments on an external view")
  }

  override def registerParameter(p: Parameter[_]): Unit = {
    throw new IllegalArgumentException("You cannot register parameters with an external view")
  }

  //
  // Delegate other view methods to wrapped view
  //

  storageFormat = view.storageFormat
  additionalStoragePathPrefix = view.additionalStoragePathPrefix
  additionalStoragePathSuffix = view.additionalStoragePathSuffix
  registeredExports = view.registeredExports
  isMaterializeOnce = view.isMaterializeOnce

  override def namingBase: String = view.namingBase

  override def comment: Option[String] = view.comment

  override def fields: Seq[Field[_]] = view.fields

  override def nameOf[F <: FieldLike[_]](f: F): Option[String] = view.nameOf(f)

  override def shortUrlPath: String = view.shortUrlPath

  override def urlPath: String = view.urlPath

  override def urlPathPrefix: String = view.urlPathPrefix

  override def lowerCasePackageName: String = view.lowerCasePackageName

  override def partitionValues(ignoreSuffixPartitions: Boolean): List[String] = view.partitionValues(ignoreSuffixPartitions)

  override def partitionParameters: Seq[Parameter[_]] = view.partitionParameters

  override def n: String = view.n

  override def nWithoutPartitioningSuffix: String = view.nWithoutPartitioningSuffix

  override def hasSuffixPartitions: Boolean = view.hasSuffixPartitions

  override def suffixPartitionParameters: Seq[Parameter[_]] = view.suffixPartitionParameters

  override def isPartition(p: Parameter[_]): Boolean = view.isPartition(p)

  override def isSuffixPartition(p: Parameter[_]): Boolean = view.isSuffixPartition(p)

  override def module: String = view.module

  override def dbName: String = view.dbName

  override def tableName: String = view.tableName

  override def dbPath: String = view.dbPath

  override def tablePath: String = view.tablePath

  override def partitionPath: String = view.partitionPath

  override def fullPath: String = view.fullPath

  override def avroSchemaPathPrefix: String = view.avroSchemaPathPrefix

  override def isPartitioned(): Boolean = view.isPartitioned()

  override def partitionSpec: String = view.partitionSpec

  override def asTableSuffix[P <: Parameter[_]](p: P): P = view.asTableSuffix(p)

  override def ensureRegisteredParameters: Unit = view.ensureRegisteredParameters

  override def parameters: Seq[Parameter[_]] = view.parameters

  override def fieldsAndParameters: Seq[FieldLike[_]] = view.fieldsAndParameters

  override def lineage: DependencyMap = view.lineage

  override def tryLineage: Try[DependencyMap] = view.tryLineage

  override def hasExternalDependencies: Boolean = view.hasExternalDependencies

}