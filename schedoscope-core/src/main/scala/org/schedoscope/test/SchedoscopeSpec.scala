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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers, Suite}
import org.schedoscope.dsl.{Field, FieldLike}
import org.schedoscope.test.resources.{LocalTestResources, TestResources}

import scala.collection.mutable.ListBuffer

trait SchedoscopeSpec extends FlatSpec with SchedoscopeSuite with Matchers

trait SchedoscopeSuite
  extends BeforeAndAfterAll
    with BeforeAndAfterEach {
  this: Suite =>

  val views = ListBuffer.empty[test]

  override protected def beforeAll(configMap: org.scalatest.ConfigMap) = {
    views.foreach {
      v => v.`then`()
    }
    super.beforeAll(configMap)
  }

  def putViewUnderTest[T <: test](view: T): T = {
    views += view
    view
  }

}

trait ReusableFixtures
  extends BeforeAndAfterEach
    with AccessRowData {
  this: Suite =>

  var resources: TestResources = new LocalTestResources
  val rowData = new ListBuffer[Map[String, Any]]()

  override protected def afterEach(): Unit = {
    rowIdx = 0
    rowData.clear()
    super.afterEach()
  }

  def then(view: LoadableView, sortedBy: FieldLike[_] = null,
           disableDependencyCheck: Boolean = false,
           disableTransformationValidation: Boolean = false) {

    view.resources = resources
    view.inputFixtures.foreach { v =>
      v.resources = resources
    }

    view.loadLocalResources()
    view.localResources.clear()

    TestUtils.loadView(view, null, false, false)

    rowData.appendAll(view.rowData)

    view.inputFixtures.foreach { v =>
      v.rowData.clear()
    }
  }

  def v[T](f: Field[T], v: T) = (f, v)

}