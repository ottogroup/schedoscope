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
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Parameter
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedAction
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.test.resources.LocalTestResources

class MorphlineDriverTest extends FlatSpec with Matchers {
  lazy val driver: MorphlineDriver = new LocalTestResources().morphlineDriver
  lazy val inputView = MorphlineView()
  lazy val failingView = FailingMorphlineView()
  lazy val compileView = CompilingMorphlineView()
  lazy val redisView = RedisMorphlineView()

  "MorphlineDriver" should "have transformation name morphline" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "morphline"
  }

  it should "execute Morphline transformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(inputView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another Morphline transformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(inputView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Morphline transformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(failingView.transformation().asInstanceOf[MorphlineTransformation])

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "execute Morphline transformations asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(inputView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute Morphline transformations and return errors when running asynchronously" taggedAs (DriverTests) in {
    val driverRunHandle = driver.run(failingView.transformation().asInstanceOf[MorphlineTransformation])

    var runWasAsynchronous = false

    while (driver.getDriverRunState(driverRunHandle).isInstanceOf[DriverRunOngoing[_]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    driver.getDriverRunState(driverRunHandle) shouldBe a[DriverRunFailed[_]]
  }
}