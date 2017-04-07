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

import org.schedoscope.dsl.storageformats._
import org.schedoscope.dsl.transformations.Transformation

/**
  * A trait summarizing the DSL constructs available for the definition of views.
  */
trait ViewDsl extends StructureDsl {

  /**
    * Add dependencies to the given view. This is done with an anonymous function returning a sequence of views the
    * current view depends on.
    */
  def dependsOn[V <: View : Manifest](dsf: () => Seq[V]): Unit

  /**
    * Add a dependency to the given view. This is done with an anonymous function returning a view the
    * current view depends on. This function is returned so that it can be assigned to variables for further reference.
    */
  def dependsOn[V <: View : Manifest](df: () => V): () => V

  /**
    * Set the transformation with which the view is created. Provide an anonymous function returning the transformation.
    * NoOp is the default transformation if none is specified.
    */
  def transformVia(ft: () => Transformation): Unit

  /**
    * Registers an export transformation with the view. You need to provide an anonymous constructor function returning this transformation.
    * This transformation is executed after the "real" transformation registered with transformVia() has executed successfully.
    */
  def exportTo(ft: () => Transformation): Unit

  /**
    * Specifiy the storage format of the view, with TextFile being the default. One can optionally specify storage path prefixes and suffixes.
    */
  def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null): Unit

  /**
    * Specify table properties of a view, which is implemented in Hive as clause TBLPROPERTIES
    */
  def tblProperties(m: Map[String, String]): Unit

  /**
    * Declare that a parameter of the view is implemented not as a Hive partition parameter but as a table name suffix.
    */
  def asTableSuffix[P <: Parameter[_]](p: P): P

  /**
    * Specify that a field or parameter is privacy sensity, which causes its hashing when exported via morphline transformations.
    */
  def privacySensitive[P <: FieldLike[_]](ps: P): P = {
    ps.isPrivacySensitive = true
    ps
  }

  /**
    * Mark a view dependency as external. This functionality is used to use views which are on
    * managed by a different Schedoscope instance. The external dependency can not be materialized,
    * but Schedoscope will fetch the current status from the Metastore each time the view receives a materialize command.
    *
    * @param view to be handled as external
    * @return a wrapped version of the original view
    */
  def external(view: View) = ExternalView(view)

  /**
    * Materialize once makes sure that the given view is only materialized once, even if its dependencies or version checksum change
    * afterwards.
    */
  def materializeOnce: Unit

  /**
    * Pluggable builder function that returns the name of the module the view belongs to.
    * The default implemementation returns the view's package in database-friendly lower-case underscore format, replacing all . with _.
    */
  var moduleNameBuilder: () => String

  /**
    * Pluggable builder function that returns the database name for the view given an environment.
    * The default implementation prepends the environment to the result of moduleNameBuilder with an underscore.
    */
  var dbNameBuilder: String => String

  /**
    * Pluggable builder function that returns the table name for the view given an environment.
    * The default implementation appends the view's name n to the result of dbNameBuilder.
    */
  var tableNameBuilder: String => String

  /**
    * Pluggable builder function that returns the HDFS path representing the database of the view given an environment.
    * The default implementation does this by building a path from the lower-case-underscore format of
    * moduleNameBuilder, replacing _ with / and prepending hdp/dev/ for the default dev environment.
    */
  var dbPathBuilder: String => String

  /**
    * Pluggable builder function that returns the HDFS path to the table the view belongs to.
    * The default implementation does this by joining dbPathBuilder and n. The latter will
    * be surrounded by additionalStoragePathPrefix and additionalStoragePathSuffix, if set.
    */
  var tablePathBuilder: String => String

  /**
    * Pluggable builder function that returns the relative partition path for the view. By default,
    * this is the standard Hive /partitionColumn=value/... pattern.
    */
  var partitionPathBuilder: () => String

  /**
    * Pluggable builder function that returns the full HDFS path to the partition represented by the view.
    * The default implementation concatenates the output of tablePathBuilder and partitionPathBuilder for
    * this purpose.
    */
  var fullPathBuilder: String => String

  /**
    * Pluggable builder function returning a path prefix of where Avro schemas can be found in HDFS.
    * By default, this is hdfs:///hdp/$\{env\}/global/datadictionary/schema/avro
    */
  var avroSchemaPathPrefixBuilder: String => String
}
