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

trait TestableView extends FillableView {}

/**
  * This trait implements most of the schedoscope test DSL. it extends View
  * with methods to generate test data, execute local hive and assertions
  *
  */
trait test extends TestableView {

  val inputFixtures = ListBuffer[View with rows]()

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
    //dependencyCheck
    if (!disableDependencyCheck) {
      if (!checkDependencies()) {
        throw new IllegalArgumentException("The input views to the test given by basedOn() do not cover all types of dependencies of the view under test.")
      }
    }

    inputFixtures.foreach(_.createViewTableAndWriteTestData())

    createViewTable()

    if (isPartitioned()) {
      resources.crate.createPartition(this)
    }

    val declaredTransformation = registeredTransformation()

    //transformation validation
    if (!disableTransformationValidation)
      declaredTransformation.validateTransformation()

    val transformationRiggedForTest = resources
      .driverFor(declaredTransformation)
      .rigTransformationForTest(declaredTransformation, resources)

    this.registeredTransformation = () => transformationRiggedForTest

    //
    // Patch export configurations to point to the test metastore with no kerberization.
    //
    configureExport("schedoscope.export.isKerberized", false)
    configureExport("schedoscope.export.kerberosPrincipal", "")
    configureExport("schedoscope.export.metastoreUri", resources.metastoreUri)

    val finalTransformationToRun = this.transformation()

    resources
      .driverFor(finalTransformationToRun)
      .runAndWait(finalTransformationToRun)

    populate(if (sortedBy != null) Some(sortedBy) else None)
  }

  /**
    * Adds dependencies for this view
    */
  def basedOn(d: View with rows*) {
    d.foreach(el => el.resources = resources)
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
    * Keeps the index of the current row
    */
  var rowIdx = 0

  /**
    * Get the value of a file in the current row
    *
    * @param f FieldLike to select the field (by name)
    *
    */
  def v[T](f: FieldLike[T]): T = {
    if (rowData(rowIdx).get(f.n).isEmpty)
      None.asInstanceOf[T]
    else
      rowData(rowIdx).get(f.n).get.asInstanceOf[T]
  }

  /**
    * Get multiple values from a multi-valued field
    *
    */
  def vs(f: FieldLike[_]): Array[(FieldLike[_], Any)] = {
    rowData(rowIdx).get(f.n).get.asInstanceOf[Array[(FieldLike[_], Any)]]
  }

  /**
    * Just increments the row index, new values that are injected by set
    * will be added to the next row
    */
  def row(fields: Unit*) {
    rowIdx = rowIdx + 1
  }

  /* (non-Javadoc)
   * @see org.schedoscope.test.rows#rowId()
  */
  override def rowId(): String = {
    rowIdPattern.format(rowIdx)
  }

  /**
    * Retrieves a Structure from a list of structures. Only one use case is
    * implemented: if a view has a fieldOf[List[Structure]], an element
    * of this list can be selected like this path(ListOfStructs, 1)
    *
    */
  def path(f: Any*): Map[String, Any] = {
    var currObj: Any = v(f.head.asInstanceOf[FieldLike[_]])
    f.tail.foreach(p => {
      p match {
        case i: Int =>
          currObj = currObj.asInstanceOf[List[Any]](i)
        case fl: FieldLike[_] => {
          if (classOf[Structure].isAssignableFrom(fl.t.runtimeClass)) {
            // TODO
          } else if (fl.t.runtimeClass == classOf[List[_]]) {
            // TODO
          } else if (fl.t.runtimeClass == classOf[Map[_, _]]) {
            // TODO
          }
        }
      }
    })
    currObj.asInstanceOf[Map[String, Any]]

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

  def withResource(res: (String, String)*) {
    res.foreach(r => {
      val prop = r._1
      val file = r._2
      val fs = resources.fileSystem
      val src = new Path(file)
      val target = new Path(s"${resources.remoteTestDirectory}/${src.getName}")
      if (fs.exists(target))
        fs.delete(target, false)
      fs.copyFromLocalFile(src, target)
      configureTransformation(prop, target.toString.replaceAll("^file:/", "file:///"))
    })
  }
}
