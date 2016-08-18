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
import org.schedoscope.dsl.transformations.{HiveTransformation, SeqTransformation, Transformation}
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._

class SeqDriverTest extends FlatSpec with Matchers {
  lazy val driver = new LocalTestResources().driverFor[SeqTransformation[Transformation, Transformation]]("seq")

  "SeqDriver" should "have transformation name seq" in {
    driver.transformationName shouldBe "seq"
  }

  it should "execute seq transformations synchronously" in {

    val driverRunState = driver.runAndWait(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("SHOW TABLES")))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another seq transformations synchronously" in {
    val driverRunState = driver.runAndWait(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("SHOW TABLES")))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute seq transformations and return errors when running synchronously" in {
    val driverRunState = driver.runAndWait(SeqTransformation(HiveTransformation("FAIL ME"), HiveTransformation("SHOW TABLES")))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute seq transformations asynchronously" in {
    val driverRunHandle = driver.run(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("SHOW TABLES")))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "succeed when first transformation is driving and second errorneous" in {
    val driverRunHandle = driver.run(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("FAIL ME")))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "fail when first transformation is not driving and second errorneous" in {
    val driverRunHandle = driver.run(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("FAIL ME"), false))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "execute seq transformations and return errors when running asynchronously" in {
    val driverRunHandle = driver.run(SeqTransformation(HiveTransformation("FAIL ME"), HiveTransformation("SHOW TABLES")))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" in {
    val runHandle = driver.run(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("SHOW TABLES")))

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" in {
    val runHandle = driver.run(SeqTransformation(HiveTransformation("SHOW TABLES"), HiveTransformation("SHOW TABLES")))

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}