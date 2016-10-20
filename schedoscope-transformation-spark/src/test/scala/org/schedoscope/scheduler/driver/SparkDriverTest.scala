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

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.dsl.transformations.SparkTransformation
import org.schedoscope.dsl.transformations.SparkTransformation.{jarOf, classNameOf}
import org.schedoscope.spark.test.{FailingSimpleFileWriter, SimpleFileWriter}
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._

class SparkDriverTest extends FlatSpec with Matchers with BeforeAndAfter {

  lazy val testResources = new LocalTestResources()

  lazy val driver = testResources.driverFor[SparkTransformation]("spark")

  lazy val successfulSparkTransformation = driver.rigTransformationForTest(
    SparkTransformation(
    classNameOf(SimpleFileWriter),
    jarOf(SimpleFileWriter),
    classNameOf(SimpleFileWriter),
    List(outpath, "one argument", "another argument")
  ), testResources)

  lazy val failingSparkTransformation = driver.rigTransformationForTest(
    SparkTransformation(
      classNameOf(FailingSimpleFileWriter),
      jarOf(FailingSimpleFileWriter),
      classNameOf(FailingSimpleFileWriter),
      List(outpath, "one argument", "another argument")
    ), testResources)

  var outpath: String = null

  before {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(tempFolder, "sparktest-" + System.nanoTime)
    } while (!folder.mkdir())

    outpath = folder.toString

    FileUtils.deleteDirectory(new File(outpath))
  }


  after {
    FileUtils.deleteDirectory(new File(outpath))
  }

  "SparkDriver" should "have transformation name spark" in {
    driver.transformationName shouldBe "spark"
  }

  it should "execute Spark transformations synchronously" in {

    val driverRunState = driver.runAndWait(successfulSparkTransformation)

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Spark transformations asynchronously" in {
    val driverRunHandle = driver.run(successfulSparkTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[SparkTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Spark transformations and return errors while running synchronously" in {
    val driverRunState = driver.runAndWait(failingSparkTransformation)

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute Spark transformations and return errors while running asynchronously" in {
    val driverRunHandle = driver.run(failingSparkTransformation)

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "be able to kill running Spark transformations" in {
    val driverRunHandle = driver.run(successfulSparkTransformation)
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunOngoing[_]]
    driver.killRun(driverRunHandle)

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[SparkTransformation]]) {
      Thread.sleep(1000)
    }

    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" in {
    val runHandle = driver.run(successfulSparkTransformation)

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" in {
    val runHandle = driver.run(successfulSparkTransformation)

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}
