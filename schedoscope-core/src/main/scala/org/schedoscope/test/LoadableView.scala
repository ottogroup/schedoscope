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

import org.apache.hadoop.fs.Path
import org.schedoscope.dsl.{FieldLike, Structure, View}

import scala.collection.mutable.ListBuffer

/**
  * This trait enables a view to be loaded with the results of it's
  * transformation during tests
  */
trait LoadableView extends WriteableView {

  val inputFixtures = ListBuffer.empty[View with WriteableView]
  val localResources = ListBuffer.empty[(String, String)]

  /**
    * Adds dependencies for this view
    */
  def basedOn(d: View with WriteableView*) {
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

  /**
    *
    */
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
    * Configures the associated transformation with the given property (as
    * multiple key value pairs)
    */
  def withConfiguration(c: (String, Any)*) {
    c.foreach(e => this.configureTransformation(e._1, e._2))
  }

  /**
    * Add a local resource
    * @param res
    */
  def withResource(res: (String, String)*) {
    localResources ++= res
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
      disableTransformationValidation = false)
  }

  /**
    * Execute the hive query in test on previously specified test fixtures.
    *
    * @param sortedBy               sort the table by field
    * @param disableDependencyCheck disable dependency checks
    */
  def `then`(sortedBy: FieldLike[_] = null,
             disableDependencyCheck: Boolean = false,
             disableTransformationValidation: Boolean = false) {
    loadLocalResources()
    TestUtils.loadView(this,
      sortedBy,
      disableDependencyCheck,
      disableTransformationValidation)
  }

  override def rowId() = {
    WriteableView.rowIdPattern.format(rowIdx)
  }

  override def numRows() = {
    rowData.size
  }
}