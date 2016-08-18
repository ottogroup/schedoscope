package org.schedoscope.scheduler.driver

import java.io.File

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.transformations.ShellTransformation
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._

import scala.io.Source

class ShellDriverTest extends FlatSpec with Matchers {

  lazy val driver = new LocalTestResources().driverFor[ShellTransformation]("shell")

  "ShellDriver" should "have transformation name shell" in {
    driver.transformationName shouldBe "shell"
  }

  it should "execute shell transformations synchronously" in {
    val driverRunState = driver.runAndWait(ShellTransformation("ls -l > /dev/null"))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute another shell transformations synchronously" in {
    val driverRunState = driver.runAndWait(ShellTransformation("ls -ld > /dev/null"))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "pass environment to the shell" in {
    val file = File.createTempFile("_schedoscope", ".sh")
    file.deleteOnExit()

    val driverRunState = driver.runAndWait(
      ShellTransformation("echo $testvar" + s">${file.getAbsolutePath()}")
        .configureWith(Map("testvar" -> "foobar"))
        .asInstanceOf[ShellTransformation])

    Source.fromFile(file).getLines.next shouldBe "foobar"
    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute shell transformations and return errors when running synchronously" in {
    val driverRunState = driver.runAndWait(ShellTransformation("exit 1"))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" in {
    val runHandle = driver.run(ShellTransformation("#"))

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" in {
    val runHandle = driver.run(ShellTransformation("#"))

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}
