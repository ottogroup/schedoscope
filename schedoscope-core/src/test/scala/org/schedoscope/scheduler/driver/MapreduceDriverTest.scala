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
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.dsl.transformations.MapreduceTransformation
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.nio.file.Files
import org.schedoscope.dsl.transformations.FailingMapper
import org.schedoscope.dsl.transformations.FailingMapper
import org.schedoscope.test.resources.LocalTestResources
import org.apache.hadoop.fs.Path
import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import org.schedoscope.scheduler.driver.TestFolder
import org.schedoscope.scheduler.driver.MapreduceDriver
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunOngoing
import org.schedoscope.dsl.transformations.FailingMapper

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

  def writeData() {
    Files.write(Paths.get(s"${inputPath("")}/file.txt"), "some data".getBytes(StandardCharsets.UTF_8))
  }

  "MapreduceDriver" should "have transformation name Mapreduce" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "mapreduce"
  }

  it should "execute Mapreduce tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(MapreduceTransformation(identityJob, List(), Map()))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another Mapreduce tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(MapreduceTransformation(identityJob, List(), Map()))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Mapreduce tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(MapreduceTransformation(invalidJob, List(), Map()))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute Mapreduce tranformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(MapreduceTransformation(identityJob, List(), Map()))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Mapreduce tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(MapreduceTransformation(failingJob, List(), Map()))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    // runWasAsynchronous shouldBe true FIXME: isn't asynchronous, why?
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

}
