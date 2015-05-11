package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.ottogroup.bi.soda.DriverTests
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.dsl.transformations.PigTransformation

class PigDriverTest extends FlatSpec with Matchers {
  lazy val driver: PigDriver = new LocalTestResources().pigDriver

  "PigDriver" should "have transformation name pig" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "pig"
  }

  it should "execute pig tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(PigTransformation("set output.compression.enabled true;", Map()))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

//  it should "execute another hive tranformations synchronously" taggedAs (DriverTests) in {
//    val driverRunState = driver.runAndWait(HiveTransformation("SHOW TABLES"))
//
//    driverRunState shouldBe a[DriverRunSucceeded[_]]
//  }
//
//  it should "execute hive tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
//    val driverRunState = driver.runAndWait(HiveTransformation("FAIL ME"))
//
//    driverRunState shouldBe a[DriverRunFailed[_]]
//  }
//
//  it should "execute hive tranformations asynchronously" taggedAs (DriverTests) in {
//    val driverRunHandle = driver.run(HiveTransformation("SHOW TABLES"))
//
//    var runWasAsynchronous = false
//
//    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
//      runWasAsynchronous = true
//
//    runWasAsynchronous shouldBe true
//    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
//  }
//
//  it should "execute hive tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
//    val driverRunHandle = driver.run(HiveTransformation("FAIL ME"))
//
//    var runWasAsynchronous = false
//
//    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
//      runWasAsynchronous = true
//
//    runWasAsynchronous shouldBe true
//    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
//  }
}