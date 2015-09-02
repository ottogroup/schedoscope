package org.schedoscope.scheduler.driver
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.DriverTests
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.dsl.transformations.ShellTransformation
import org.schedoscope.DriverSettings
import com.typesafe.config.ConfigFactory
import org.schedoscope.ShellTests
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter.driverRunCompletionHandlerCalled

class ShellDriverTest extends FlatSpec with Matchers {

  lazy val driver: ShellDriver = new LocalTestResources().shellDriver

  "ShellDriver" should "have transformation name shell" taggedAs (DriverTests, ShellTests) in {
    driver.transformationName shouldBe "shell"
  }

  it should "execute shell tranformations synchronously" taggedAs (DriverTests, ShellTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("#"))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another shell tranformations synchronously" taggedAs (DriverTests, ShellTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("echo test >> /tmp/testout"))
    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute pig tranformations and return errors when running synchronously" taggedAs (DriverTests, ShellTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("exit 1"))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(ShellTransformation("#"))

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletionHandlerCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }
}
