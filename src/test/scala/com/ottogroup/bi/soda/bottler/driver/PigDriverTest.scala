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
    val driverRunState = driver.runAndWait(PigTransformation("/* a comment */", Map()))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another pig tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(PigTransformation("/* a comment */", Map()))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }
  

  it should "execute pig tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(PigTransformation("FAIL ME", Map()))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute pig tranformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(PigTransformation("/* a comment */", Map()))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute pig tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(PigTransformation("FAIL ME", Map()))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }
}