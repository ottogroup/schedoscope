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

import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, Paths }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.scalatest.{ FlatSpec, Matchers }
import org.schedoscope.DriverTests
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{ FailingMapper, MapreduceTransformation }
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._

class MapreduceDriverTest extends FlatSpec with Matchers with TestFolder {
  lazy val driver: MapreduceDriver = new LocalTestResources().mapreduceDriver

  def invalidJob: (Map[String, Any]) => Job = (m: Map[String, Any]) => Job.getInstance

  def failingJob: (Map[String, Any]) => Job = (m: Map[String, Any]) => {
    writeData()
    val job = Job.getInstance
    job.setMapperClass(classOf[FailingMapper])
    FileInputFormat.setInputPaths(job, new Path(inputPath("")))
    FileOutputFormat.setOutputPath(job, new Path(outputPath(System.nanoTime.toString)))
    job
  }

  def identityJob: (Map[String, Any]) => Job = (m: Map[String, Any]) => {
    writeData()
    val job = Job.getInstance
    FileInputFormat.setInputPaths(job, new Path(inputPath("")))
    FileOutputFormat.setOutputPath(job, new Path(outputPath(System.nanoTime.toString)))
    job
  }

  case class DummyView() extends View

  def writeData() {
    Files.write(Paths.get(s"${inputPath("")}/file.txt"), "some data".getBytes(StandardCharsets.UTF_8))
  }

  "MapreduceDriver" should "have transformation name Mapreduce" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "mapreduce"
  }

  it should "execute Mapreduce tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(MapreduceTransformation(new DummyView(), identityJob))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another Mapreduce tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(MapreduceTransformation(new DummyView(), identityJob))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Mapreduce tranformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(MapreduceTransformation(new DummyView(), identityJob))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Mapreduce tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(MapreduceTransformation(new DummyView(), failingJob))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    // runWasAsynchronous shouldBe true FIXME: isn't asynchronous, why?
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(MapreduceTransformation(new DummyView(), identityJob))

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(MapreduceTransformation(new DummyView(), identityJob))

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}
