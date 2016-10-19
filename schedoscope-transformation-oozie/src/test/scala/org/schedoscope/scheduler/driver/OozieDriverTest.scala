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
import org.schedoscope.dsl.transformations.OozieTransformation
import org.schedoscope.test.resources.OozieTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._

class OozieDriverTest extends FlatSpec with Matchers {

  lazy val resources: OozieTestResources = OozieTestResources()

  lazy val cluster = resources.mo

  lazy val driver = resources.driverFor[OozieTransformation]("oozie")

  lazy val workingOozieTransformation = driver.rigTransformationForTest(
    OozieTransformation(
      "bundle", "workflow",
      "/tmp/schedoscope/oozie/workflows/bundle/workflow/")
      .configureWith(Map(
        "jobTracker" -> cluster.getJobTrackerUri(),
        "nameNode" -> cluster.getNameNodeUri(),
        "oozie.use.system.libpath" -> "false")).asInstanceOf[OozieTransformation], resources)

  lazy val failingOozieTransformation = driver.rigTransformationForTest(
    OozieTransformation(
      "bundle", "failflow",
      "/tmp/schedoscope/oozie/workflows/bundle/failflow/")
      .configureWith(Map(
        "jobTracker" -> cluster.getJobTrackerUri(),
        "nameNode" -> cluster.getNameNodeUri(),
        "oozie.use.system.libpath" -> "false")).asInstanceOf[OozieTransformation], resources)

  "Oozie" should "have transformation name oozie" in {
    driver.transformationName shouldBe "oozie"
  }

  it should "execute Oozie transformations synchronously" in {
    val driverRunState = driver.runAndWait(workingOozieTransformation)

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Oozie transformations asynchronously" in {
    val driverRunHandle = driver.run(workingOozieTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Oozie transformations and return errors while running synchronously" in {
    val driverRunState = driver.runAndWait(failingOozieTransformation)

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute Oozie transformations and return errors while running asynchronously" in {
    val driverRunHandle = driver.run(failingOozieTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "be able to kill running Oozie transformations" in {
    val driverRunHandle = driver.run(workingOozieTransformation)
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunOngoing[_]]
    driver.killRun(driverRunHandle)

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]]) {
      Thread.sleep(1000)
    }

    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" in {
    val runHandle = driver.run(workingOozieTransformation)

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" in {
    val runHandle = driver.run(workingOozieTransformation)

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}