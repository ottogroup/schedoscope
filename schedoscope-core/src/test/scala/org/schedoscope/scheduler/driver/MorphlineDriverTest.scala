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
package org.schedoscope.scheduler.driver

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.DriverTests
import org.schedoscope.dsl.transformations.MorphlineTransformation
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.dsl.transformations.MorphlineTransformation
import test.eci.datahub.MorphlineView
import org.schedoscope.dsl.transformations.MorphlineTransformation
import test.eci.datahub._
import test.eci.datahub.CompilingMorphlineView
import org.schedoscope.dsl.View
import org.schedoscope.dsl.Parameter
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedAction
import org.schedoscope.test.resources.LocalTestResources
import test.eci.datahub.RedisMorphlineView
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter.driverRunCompletionHandlerCalled

class MorphlineDriverTest extends FlatSpec with Matchers {
  lazy val driver: MorphlineDriver = new LocalTestResources().morphlineDriver
  lazy val inputView = MorphlineView()
  lazy val failingView = FailingMorphlineView()
  lazy val compileView = CompilingMorphlineView()
  lazy val redisView = RedisMorphlineView()

  "MorphlineDriver" should "have transformation name morphline" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "morphline"
  }

  it should "execute Morphline transformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(inputView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another Morphline transformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(inputView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Morphline transformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(failingView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute Morphline transformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(inputView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Morphline transformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(failingView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(inputView.transformation().asInstanceOf[MorphlineTransformation])

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletionHandlerCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }
}