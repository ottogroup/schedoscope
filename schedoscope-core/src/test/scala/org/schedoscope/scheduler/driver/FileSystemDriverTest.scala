/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.driver

import java.io.File

import org.scalatest.{ FlatSpec, Matchers }
import org.schedoscope.DriverTests
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.{ FilesystemTransformation, _ }
import org.schedoscope.test.resources.LocalTestResources
import org.schedoscope.test.resources.TestDriverRunCompletionHandlerCallCounter.driverRunCompletionHandlerCalled
import test.eci.datahub.Product

class FileSystemDriverTest extends FlatSpec with Matchers with TestFolder {
  lazy val driver: FileSystemDriver = new LocalTestResources().fileSystemDriver

  "FileSystemDriver" should "be have transformation name filesystem" taggedAs (DriverTests) in {
    driver.transformationName shouldBe "filesystem"
  }

  it should "execute Copy file transformation with a single file" taggedAs (DriverTests) in {
    createInputFile("aTest.file")

    outputFile("aTest.file") should not be 'exists

    driver.runAndWait(Copy(inputPath("aTest.file"), out, false)) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
  }

  it should "execute Copy file transformation of multiple files with pattern" taggedAs (DriverTests) in {
    createInputFile("aTest.file")
    createInputFile("anotherTest.file")

    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") should not be 'exists

    driver.runAndWait(Copy(inputPath("*.file"), out, false)) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
    outputFile("anotherTest.file") shouldBe 'exists
  }

  it should "execute Copy file transformation recursively" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")
    createInputFile(s"subfolder${/}anotherSubfolder${/}anotherTest.file")

    outputFile(s"subfolder${/}aTest.file") should not be 'exists
    outputFile(s"subfolder${/}anotherSubfolder${/}anotherTest.file") should not be 'exists

    driver.runAndWait(Copy(inputPath("/*"), out, true)) shouldBe a[DriverRunSucceeded[_]]

    outputFile(s"subfolder${/}aTest.file") shouldBe 'exists
    outputFile(s"subfolder${/}anotherSubfolder${/}anotherTest.file") shouldBe 'exists
  }

  it should "execute Move file transformation with a single file" taggedAs (DriverTests) in {
    createInputFile("aTest.file")

    inputFile("aTest.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists

    driver.runAndWait(Move(inputPath("aTest.file"), out)) shouldBe a[DriverRunSucceeded[_]]

    inputFile("aTest.file") should not be 'exists
    outputFile("aTest.file") shouldBe 'exists
  }

  it should "execute Move file transformation with a folder recursively" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")

    inputFile(s"subfolder${/}aTest.file") shouldBe 'exists
    outputFile(s"subfolder${/}aTest.file") should not be 'exists

    driver.runAndWait(Move(inputPath("subfolder"), out)) shouldBe a[DriverRunSucceeded[_]]

    inputFile(s"subfolder") should not be 'exists
    outputFile(s"subfolder${/}aTest.file") shouldBe 'exists
  }

  it should "execute IfExists file transformation when a given file exists" taggedAs (DriverTests) in {
    createInputFile("check.file")

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") should not be 'exists

    driver.runAndWait(IfExists(inputPath("check.file"), Touch(outputPath("aTest.file")))) shouldBe a[DriverRunSucceeded[_]]
    driver.runAndWait(IfExists(inputPath("anotherCheck.file"), Touch(outputPath("anotherTest.file")))) shouldBe a[DriverRunSucceeded[_]]

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") shouldBe 'exists
    outputFile("anotherTest.file") should not be 'exists
  }

  it should "execute IfNotExists file transformation when a given file does not exist" taggedAs (DriverTests) in {
    createInputFile("check.file")

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") should not be 'exists

    driver.runAndWait(IfNotExists(inputPath("check.file"), Touch(outputPath("aTest.file")))) shouldBe a[DriverRunSucceeded[_]]
    driver.runAndWait(IfNotExists(inputPath("anotherCheck.file"), Touch(outputPath("anotherTest.file")))) shouldBe a[DriverRunSucceeded[_]]

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") shouldBe 'exists
  }

  it should "execute Touch file transformation" taggedAs (DriverTests) in {
    outputFile("aTest.file") should not be 'exists

    driver.runAndWait(Touch(outputPath("aTest.file"))) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
  }

  it should "execute Delete file transformations on single files" taggedAs (DriverTests) in {
    createInputFile("aTest.file")
    inputFile("aTest.file") shouldBe 'exists

    driver.runAndWait(Delete(inputPath("aTest.file"))) shouldBe a[DriverRunSucceeded[_]]

    inputFile("aTest.file") should not be 'exists
  }

  it should "execute Delete file transformations on folders recursively" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")

    inputFile(s"subfolder${/}aTest.file") shouldBe 'exists

    driver.runAndWait(Delete(inputPath("subfolder"), true)) shouldBe a[DriverRunSucceeded[_]]

    inputFile("subfolder") should not be 'exists
  }

  it should "execute CopyFrom file transformations by copying a single file to partition path of view" taggedAs (DriverTests) in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out
    }

    createInputFile("aTest.file")
    new File(s"${product.fullPath}${/}aTest.file") should not be 'exists

    driver.runAndWait(CopyFrom(inputPath("*.file"), product, false)) shouldBe a[DriverRunSucceeded[_]]

    new File(s"${product.fullPath}${/}aTest.file") shouldBe 'exists
  }

  it should "execute CopyFrom file transformations by copying a single file from java resources to partition path of view" taggedAs (DriverTests) in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out
    }

    new File(s"${product.fullPath}${/}classpathtest.txt") should not be 'exists

    driver.runAndWait(CopyFrom("classpath://input/classpathtest.txt", product, false)) shouldBe a[DriverRunSucceeded[_]]

    new File(s"${product.fullPath}${/}classpathtest.txt") shouldBe 'exists
  }

  it should "execute StoreFrom file transformations by copying an input stream to partition path of view" taggedAs (DriverTests) in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out
    }

    new File(s"${product.fullPath}${/}stream.out") should not be 'exists

    driver.runAndWait(StoreFrom(this.getClass().getResourceAsStream("/input/classpathtest.txt"), product)) shouldBe a[DriverRunSucceeded[_]]

    new File(s"${product.fullPath}${/}stream.out") shouldBe 'exists
  }

  it should "execute CopyFrom file transformations by copying a folder recursively to partition path of view" taggedAs (DriverTests) in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out
    }

    createInputFile(s"subfolder${/}aTest.file")
    new File(s"${product.fullPath}${/}aTest.file") should not be 'exists

    driver.runAndWait(CopyFrom(inputPath("subfolder"), product, true)) shouldBe a[DriverRunSucceeded[_]]

    new File(s"${product.fullPath}${/}subfolder${/}aTest.file") shouldBe 'exists
  }

  it should "run asynchronously" taggedAs (DriverTests) in {
    outputFile("aTest.file") should not be 'exists

    val runHandle = driver.run(Touch(outputPath("aTest.file")))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[FilesystemTransformation]])
      runWasAsynchronous = true

    driver.getDriverRunState(runHandle) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
  }

  it should "return DriverRunFailed in case of problems when running asynchronously" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")
    inputFile("subfolder") shouldBe 'exists

    val runHandle = driver.run(Delete(inputPath("subfolder"), false))

    var runWasAsynchronous = false

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[FilesystemTransformation]])
      runWasAsynchronous = true

    driver.getDriverRunState(runHandle) shouldBe a[DriverRunFailed[_]]

    inputFile("subfolder") shouldBe 'exists
  }

  it should "return DriverRunFailed in case of problems when running synchronously" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")
    inputFile("subfolder") shouldBe 'exists

    val runState = driver.runAndWait(Delete(inputPath("subfolder"), false))

    runState shouldBe a[DriverRunFailed[_]]

    inputFile("subfolder") shouldBe 'exists
  }

  it should "call its DriverRunCompletitionHandlers upon request" taggedAs (DriverTests) in {
    val runHandle = driver.run(Touch(outputPath("aTest.file")))

    while (driver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[FilesystemTransformation]]) {}

    driver.driverRunCompleted(runHandle)

    driverRunCompletionHandlerCalled(runHandle, driver.getDriverRunState(runHandle)) shouldBe true
  }

}