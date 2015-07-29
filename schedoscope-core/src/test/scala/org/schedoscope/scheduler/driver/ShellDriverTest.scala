package org.schedoscope.scheduler.driver
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.DriverTests
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.dsl.transformations.ShellTransformation
import org.schedoscope.DriverSettings
import com.typesafe.config.ConfigFactory
class ShellDriverTest extends FlatSpec with Matchers {
  
  lazy val driver: ShellDriver = new ShellDriver(new DriverSettings(ConfigFactory.empty(),"shell"))

  
    "ShellDriver" should "have transformation name shell" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "shell"
  }

  it should "execute shell tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("#"))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another shell tranformations synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("echo test >> /tmp/testout"))
    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }
    it should "execute pig tranformations and return errors when running synchronously" taggedAs (DriverTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("exit 1"))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }
}