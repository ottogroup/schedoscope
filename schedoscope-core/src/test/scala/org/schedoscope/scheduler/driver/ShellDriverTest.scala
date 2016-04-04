package org.schedoscope.scheduler.driver

import java.io.File
import org.scalatest.{ FlatSpec, Matchers }
import org.schedoscope.{ DriverTests, ShellTests }
import org.schedoscope.dsl.transformations.ShellTransformation
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter._
import scala.io.Source
import org.schedoscope.dsl.transformations.ShellTransformation

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
    val driverRunState = driver.runAndWait(ShellTransformation("echo error >/dev/stderr;zcat /usr/share/man/man1/*gz"))

    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "pass environment to the shell" taggedAs (DriverTests, ShellTests) in {
    val file = File.createTempFile("_schedoscope", ".sh")
    file.deleteOnExit()

    val driverRunState = driver.runAndWait(
      ShellTransformation("echo $testvar" + s">${file.getAbsolutePath()}")
        .configureWith(Map("testvar" -> "foobar"))
        .asInstanceOf[ShellTransformation])

    Source.fromFile(file).getLines.next shouldBe "foobar"
    driverRunState shouldBe a[DriverRunSucceeded[_]]
  }

  it should "execute pig tranformations and return errors when running synchronously" taggedAs (DriverTests, ShellTests) in {
    val driverRunState = driver.runAndWait(ShellTransformation("exit 1"))

    driverRunState shouldBe a[DriverRunFailed[_]]
  }

  it should "call its DriverRunCompletitionHandlers' driverRunCompleted upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(ShellTransformation("#"))

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[_]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletedCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

  it should "call its DriverRunCompletitionHandlers' driverRunStarted upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(ShellTransformation("#"))

    driver.driverRunStarted(runHandle)

    driverRunStartedCalled(runHandle) shouldBe true
  }
}
