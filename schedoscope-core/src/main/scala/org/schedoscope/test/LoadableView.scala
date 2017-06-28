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
package org.schedoscope.test


import java.io.File
import org.apache.hadoop.fs.Path
import org.schedoscope.dsl.storageformats.Avro
import org.schedoscope.dsl.{FieldLike, View}

import scala.collection.mutable.ListBuffer

/**
  * This trait enables a view to be loaded with the results of it's
  * transformation during tests.
  */

trait LoadableView extends WritableView {

  var sortedBy: Option[FieldLike[_]] = None
  var dependencyCheckDisabled = false
  var transformationValidationDisabled = false

  val inputFixtures = ListBuffer.empty[View with WritableView]
  val localResources = ListBuffer.empty[(String, String)]

  /**
    * Fills this view with data from hive, potentially sorted by a column
    *
    * @param orderedBy the optional FieldLike for the column to sort by
    */
  def populate(orderedBy: Option[FieldLike[_]]) {
    val db = resources.database
    rowData.clear()
    rowData.appendAll(db.selectView(this, orderedBy))
  }

  /**
    * Adds dependencies for this view
    */
  def basedOn(d: View with WritableView*) {
    d.foreach { el =>
      el.resources = resources
      el.createViewTable()
    }
    //check
    inputFixtures ++= d
  }

  /**
    * Compares the dependencies of the tested view
    * and the added dependencies to the test
    *
    * @return true if dependencies match
    */
  def checkDependencies(): Boolean = {

    if (inputFixtures.isEmpty && dependencies.isEmpty) {
      return true
    }

    val dependencyNames = dependencies
      .map(v => v.dbName + "." + v.tableName)
      .distinct
      .toList

    val depNames = inputFixtures.map(v => v.dbName + "." + v.tableName)
      .distinct
      .toList

    if (depNames.length == dependencyNames.length) {
      depNames
        .map(dependencyNames.contains(_))
        .reduce(_ && _)
    } else {
      false
    }

  }

  def loadLocalResources(): Unit = {
    localResources.foreach {
      case (prop, file) =>
        val fs = resources.fileSystem
        val src = new Path(file)
        val target = new Path(s"${resources.remoteTestDirectory}/${src.getName}")
        if (fs.exists(target))
          fs.delete(target, false)
        fs.copyFromLocalFile(src, target)
        configureTransformation(prop, target.toString.replaceAll("^file:/", "file:///"))
    }
  }

  /**
    * Configures the associated transformation with the given property (as
    * key value pair)
    */
  def withConfiguration(k: String, v: Any) {
    configureTransformation(k, v)
  }

  /**
    * TODO:
    * Configures the associated transformation with the given property (as
    * key value pair)
    */
  def withConfiguration(conf: Map[String, Any]) {
    conf.foreach {
      case (k, v) =>
        configureTransformation(k, v)
    }
  }

  /**
    * Configures the associated transformation with the given property (as
    * multiple key value pairs)
    */
  def withConfiguration(c: (String, Any)*) {
    c.foreach(e => this.configureTransformation(e._1, e._2))
  }

  /**
    * Register a local resource which will be added to the configuration and
    * loaded into the hdfs during tests.
    */
  def withResource(res: (String, String)*) {
    localResources ++= res
  }

  /**
    * Disable the matching of the dependencies of the view
    * and views passed to basedOn()
    */
  def disableDependencyCheck(): Unit = {
    dependencyCheckDisabled = true
  }

  /**
    * Disable potential checks of the transformation logic
    */
  def disableTransformationValidation(): Unit = {
    transformationValidationDisabled = true
  }

  /**
    *
    * @param fieldLike field to desc
    */
  def sortRowsBy(fieldLike: FieldLike[_]): Unit = {
    sortedBy = Some(fieldLike)
  }
}

/**
  * This trait implements most of the schedoscope test DSL. it extends View
  * with methods to generate test data, execute local hive and assertions
  */
trait test extends LoadableView with AccessRowData {

  /**
    * Execute the hive query in test on previously specified test fixtures
    */
  def `then`() {
    `then`(null,
      disableDependencyCheck = false,
      disableTransformationValidation = false,
      disableLineageValidation = true)
  }

  /**
    * Execute the hive query in test on previously specified test fixtures.
    *
    * @param sortedBy                        sort the table by field
    * @param disableDependencyCheck          disable dependency checks
    * @param disableTransformationValidation disable transformation validation
    * @param disableLineageValidation        disable lineage validation
    */
  def `then`(sortedBy: FieldLike[_] = null,
             disableDependencyCheck: Boolean = false,
             disableTransformationValidation: Boolean = false,
             disableLineageValidation: Boolean = true) {
    TestUtils.loadView(this, sortedBy, disableDependencyCheck, disableTransformationValidation,
      disableLineageValidation)
  }

  override def rowId() = {
    WritableView.rowIdPattern.format(rowIdx)
  }

  override def numRows() = {
    rowData.size
  }

  override def tablePath = storageFormat match {
    case Avro(testPath, _) => new File(getClass.getResource("/" + testPath).getPath).getParentFile.getAbsolutePath

    case _ => tablePathBuilder(env)
  }

  override def avroSchemaPathPrefix = storageFormat match {
    case Avro(testPath, _) => new File(getClass.getResource("/").getPath).getAbsolutePath

    case _ => avroSchemaPathPrefixBuilder(env)
  }

}

/**
  * Syntactic sugar for [[ReusableHiveSchema]] tests
  */
trait OutputSchema extends LoadableView