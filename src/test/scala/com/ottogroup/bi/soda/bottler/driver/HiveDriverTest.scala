package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation

class HiveDriverTest extends FlatSpec with BeforeAndAfter with Matchers {
  val hiveDriver = LocalTestResources.hiveDriver

  "HiveDriver" should "be named hive" taggedAs (DriverTests) in {
    hiveDriver.name shouldBe "hive"
  }

  it should "execute hive tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = hiveDriver.runAndWait(HiveTransformation("SHOW TABLES"))

    driverRunState shouldBe a[DriverRunSucceeded[HiveTransformation]]
  }

  it should "execute hive tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = hiveDriver.runAndWait(HiveTransformation("FAIL ME"))

    driverRunState shouldBe a[DriverRunFailed[HiveTransformation]]
  }

  it should "execute hive tranformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = hiveDriver.run(HiveTransformation("SHOW TABLES"))

    var runWasAsynchronous = false

    while (hiveDriver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[HiveTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    hiveDriver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[HiveTransformation]]
  }

  it should "execute hive tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = hiveDriver.run(HiveTransformation("FAIL ME"))

    var runWasAsynchronous = false

    while (hiveDriver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[HiveTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    hiveDriver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[HiveTransformation]]
  }
}