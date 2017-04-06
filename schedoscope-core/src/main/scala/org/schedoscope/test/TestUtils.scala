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

import org.schedoscope.dsl.FieldLike
import org.schedoscope.lineage.NoHiveTransformationException

import scala.util.{Failure, Success}

object TestUtils {

  /**
    * Execute the transformation on a view
    * and ingest the resulting file into the view.
    *
    * @param sortedBy               sort the table by field
    * @param disableDependencyCheck disable dependency checks
    */
  def loadView(view: LoadableView,
               sortedBy: FieldLike[_] = null,
               disableDependencyCheck: Boolean = false,
               disableTransformationValidation: Boolean = false,
               disableLineageValidation: Boolean = true) = {


    //dependencyCheck
    if (!disableDependencyCheck) {
      if (!view.checkDependencies()) {
        throw new IllegalArgumentException("The input views to the test given by basedOn() do not cover all types of dependencies of the view under test.")
      }
    }

    // lineage validation
    if (!disableLineageValidation && view.explicitLineage.isEmpty && view.dependencies.nonEmpty) {
      view.tryLineage match {
        case Success(_) =>
        case Failure(ex) => ex match {
          case _: NoHiveTransformationException =>
          case _ => throw ex
        }
      }
    }

    view.loadLocalResources()

    val resources = view.resources
    val inputFixtures = view.inputFixtures

    // edge case to solve issue with recursive dependencies
    var recursiveView = false
    inputFixtures.foreach { v =>
      if(v.n == view.n && !recursiveView) {
        println(s"[Schedoscope INFO]: The View you are testing - ${view.n} - depends on itself " +
          s"(specified with dependsOn {} method). In these edge cases Schedoscope also changes Storage " +
          s"format of output files into TextFile() of the view being tested (besides for its input views). " +
          s"You can safely ignore this message, as this should not affect the logic of your tests.")
        view.storedAs(resources.textStorage)
        recursiveView = true
      }
    }

    inputFixtures.foreach(_.writeData())
    view.createViewTable()

    if (view.isPartitioned()) {
      resources.schemaManager.createPartition(view)
    }

    val declaredTransformation = view.registeredTransformation()

    //transformation validation
    if (!(disableTransformationValidation || view.dependencyCheckDisabled))
      declaredTransformation.validateTransformation()

    val transformationRiggedForTest = resources
      .driverFor(declaredTransformation)
      .rigTransformationForTest(declaredTransformation, resources)

    view.registeredTransformation = () => transformationRiggedForTest

    //
    // Patch export configurations to point to the test metastore with no kerberization.
    //
    view.configureExport("schedoscope.export.isKerberized", false)
    view.configureExport("schedoscope.export.kerberosPrincipal", "")
    view.configureExport("schedoscope.export.metastoreUri", resources.metastoreUri)

    val finalTransformationToRun = view.transformation()

    resources
      .driverFor(finalTransformationToRun)
      .runAndWait(finalTransformationToRun)


    if (sortedBy != null) {
      view.populate(Some(sortedBy))
    } else {
      view.populate(view.sortedBy)
    }


  }
}