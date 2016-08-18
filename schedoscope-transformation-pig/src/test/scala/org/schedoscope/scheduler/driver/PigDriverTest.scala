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

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.transformations.PigTransformation
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._

class PigDriverTest extends FlatSpec with Matchers {
  lazy val testResources = new LocalTestResources()

  lazy val driver = testResources.driverFor[PigTransformation]("pig")

  lazy val okTransformation = driver.rigTransformationForTest(PigTransformation("/* a comment */"), testResources)

  lazy val badTransformation = driver.rigTransformationForTest(PigTransformation("FAIL ME"), testResources)

  "PigDriver" should "have transformation name pig" in {
    driver.transformationName shouldBe "pig"
  }

  it should "execute pig transformations synchronously" in {
    val driverRunState = driver.runAndWait(okTransformation)

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another pig transformations synchronously" in {
    val driverRunState = driver.runAndWait(okTransformation)

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute pig transformations and return errors when running synchronously" in {
    val driverRunState = driver.runAndWait(badTransformation)

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute pig transformations asynchronously" in {
    val driverRunHandle = driver.run(okTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute pig transformations and return errors when running asynchronously" in {
    val driverRunHandle = driver.run(badTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" in {
    val runHandle = driver.run(okTransformation)

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" in {
    val runHandle = driver.run(okTransformation)

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}