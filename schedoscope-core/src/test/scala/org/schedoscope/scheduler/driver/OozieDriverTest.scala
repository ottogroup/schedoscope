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

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import org.schedoscope.DriverTests
import org.schedoscope.OozieTests
import org.schedoscope.dsl.transformations.OozieTransformation
import org.schedoscope.test.resources.OozieTestResources

class OozieDriverTest extends FlatSpec with Matchers {

  lazy val resources: OozieTestResources = OozieTestResources()

  def cluster = resources.mo
  def driver = resources.oozieDriver

  def deployWorkflow(wf: OozieTransformation) = {
    val hdfs = resources.fileSystem
    val dest = new Path(s"${resources.namenode}/${wf.workflowAppPath}")
    val src = new Path(s"src/test/resources/oozie/${wf.bundle}/${wf.workflow}/workflow.xml")

    if (!hdfs.exists(dest))
      hdfs.mkdirs(dest)

    hdfs.copyFromLocalFile(src, dest)
    wf
  }

  lazy val workingOozieTransformation = deployWorkflow(
    OozieTransformation(
      "bundle", "workflow",
      "/tmp/schedoscope/oozie/workflows/bundle/workflow/", Map(
        "jobTracker" -> cluster.getJobTrackerUri(),
        "nameNode" -> cluster.getNameNodeUri(),
        "oozie.use.system.libpath" -> "false")))

  lazy val failingOozieTransformation = deployWorkflow(
    OozieTransformation(
      "bundle", "failflow",
      "/tmp/schedoscope/oozie/workflows/bundle/failflow/", Map(
        "jobTracker" -> cluster.getJobTrackerUri(),
        "nameNode" -> cluster.getNameNodeUri(),
        "oozie.use.system.libpath" -> "false")))

  "Oozie" should "have transformation name oozie" taggedAs (DriverTests, OozieTests) in {
    driver.transformationName shouldBe "oozie"
  }

  it should "execute oozie tranformations synchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunState = driver.runAndWait(workingOozieTransformation)

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute oozie tranformations asynchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunHandle = driver.run(workingOozieTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute oozie tranformations and return errors while running synchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunState = driver.runAndWait(failingOozieTransformation)

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute oozie tranformations and return errors while running asynchronously" taggedAs (DriverTests, OozieTests) in {
    val driverRunHandle = driver.run(failingOozieTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "be able to kill running oozie transformations" taggedAs (DriverTests, OozieTests) in {
    val driverRunHandle = driver.run(workingOozieTransformation)
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunOngoing[_]]
    driver.killRun(driverRunHandle)

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[OozieTransformation]]) {
      Thread.sleep(1000)
    }

    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }
}