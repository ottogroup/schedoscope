package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.ottogroup.bi.soda.DriverTests
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import test.eci.datahub.MorphlineView
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import test.eci.datahub._
import test.eci.datahub.CompilingMorphlineView

class MorphlineDriverTest extends FlatSpec with Matchers {
  lazy val driver: MorphlineDriver = new MorphlineDriver()
  lazy val inputView = MorphlineView()
  lazy val failingView = FailingMorphlineView()
  lazy val compileView = CompilingMorphlineView()
  lazy val redisView = RedisMorphlineView()
  "MorphlineDriver" should "have transformation name Morphline" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "morphline"
  }

  it should "execute Morphline tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(inputView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }
  
  it should "execute another Morphline tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(inputView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Morphline tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(failingView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute Morphline tranformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(inputView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Morphline tranformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(failingView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }
  
  it should "compile a morphline Transformation"  taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(compileView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }
    it should "store stuff in redis"  taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(redisView.transformation().asInstanceOf[MorphlineTransformation])
    
    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }
}