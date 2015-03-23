package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.ottogroup.bi.soda.DriverTests
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.test.resources.LocalTestResources

class HiveDriverTest extends FlatSpec with Matchers {
  lazy val driver: HiveDriver = new LocalTestResources().hiveDriver

  "HiveDriver" should "have transformation name hive" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "hive"
  }

  it should "execute hive tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(HiveTransformation("SHOW TABLES"))

    driverRunState shouldBe a[DriverRunSucceeded[HiveTransformation]]
  }

  it should "execute hive tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(HiveTransformation("FAIL ME"))

    driverRunState shouldBe a[DriverRunFailed[HiveTransformation]]
  }

  it should "execute hive tranformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(HiveTransformation("SHOW TABLES"))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[HiveTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[HiveTransformation]]
  }

  it should "execute hive tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(HiveTransformation("FAIL ME"))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[HiveTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[HiveTransformation]]
  }
}