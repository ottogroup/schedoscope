package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import com.ottogroup.bi.soda.dsl.transformations.filesystem._
import java.io.File
import test.eci.datahub.Product
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import com.ottogroup.bi.soda.dsl.Parameter.p

class FileSystemDriverTest extends FlatSpec with BeforeAndAfter with Matchers with TestFolder {
  val fileSystemDriver = new FileSystemDriver(UserGroupInformation.getLoginUser(), new Configuration())

  "FileSystemDriver" should "be named filesystem" taggedAs (DriverTests) in {
    fileSystemDriver.name shouldBe "filesystem"
  }

  it should "execute Copy file transformation with a single file" taggedAs (DriverTests) in {
    createInputFile("aTest.file")

    outputFile("aTest.file") should not be 'exists

    fileSystemDriver.runAndWait(Copy(inputPath("aTest.file"), out, false)) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
  }

  it should "execute Copy file transformation of multiple files with pattern" taggedAs (DriverTests) in {
    createInputFile("aTest.file")
    createInputFile("anotherTest.file")

    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") should not be 'exists

    fileSystemDriver.runAndWait(Copy(inputPath("*.file"), out, false)) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
    outputFile("anotherTest.file") shouldBe 'exists
  }

  it should "execute Copy file transformation recursively" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")
    createInputFile(s"subfolder${/}anotherSubfolder${/}anotherTest.file")

    outputFile(s"subfolder${/}aTest.file") should not be 'exists
    outputFile(s"subfolder${/}anotherSubfolder${/}anotherTest.file") should not be 'exists

    fileSystemDriver.runAndWait(Copy(inputPath("/*"), out, true)) shouldBe a[DriverRunSucceeded[_]]

    outputFile(s"subfolder${/}aTest.file") shouldBe 'exists
    outputFile(s"subfolder${/}anotherSubfolder${/}anotherTest.file") shouldBe 'exists
  }

  it should "execute Move file transformation with a single file" taggedAs (DriverTests) in {
    createInputFile("aTest.file")

    inputFile("aTest.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists

    fileSystemDriver.runAndWait(Move(inputPath("aTest.file"), out)) shouldBe a[DriverRunSucceeded[_]]

    inputFile("aTest.file") should not be 'exists
    outputFile("aTest.file") shouldBe 'exists
  }

  it should "execute Move file transformation with a folder recursively" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")

    inputFile(s"subfolder${/}aTest.file") shouldBe 'exists
    outputFile(s"subfolder${/}aTest.file") should not be 'exists

    fileSystemDriver.runAndWait(Move(inputPath("subfolder"), out)) shouldBe a[DriverRunSucceeded[_]]

    inputFile(s"subfolder") should not be 'exists
    outputFile(s"subfolder${/}aTest.file") shouldBe 'exists
  }

  it should "execute IfExists file transformation when a given file exists" taggedAs (DriverTests) in {
    createInputFile("check.file")

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") should not be 'exists

    fileSystemDriver.runAndWait(IfExists(inputPath("check.file"), Touch(outputPath("aTest.file")))) shouldBe a[DriverRunSucceeded[_]]
    fileSystemDriver.runAndWait(IfExists(inputPath("anotherCheck.file"), Touch(outputPath("anotherTest.file")))) shouldBe a[DriverRunSucceeded[_]]

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") shouldBe 'exists
    outputFile("anotherTest.file") should not be 'exists
  }

  it should "execute IfNotExists file transformation when a given file does not exist" taggedAs (DriverTests) in {
    createInputFile("check.file")

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") should not be 'exists

    fileSystemDriver.runAndWait(IfNotExists(inputPath("check.file"), Touch(outputPath("aTest.file")))) shouldBe a[DriverRunSucceeded[_]]
    fileSystemDriver.runAndWait(IfNotExists(inputPath("anotherCheck.file"), Touch(outputPath("anotherTest.file")))) shouldBe a[DriverRunSucceeded[_]]

    inputFile("check.file") shouldBe 'exists
    outputFile("aTest.file") should not be 'exists
    outputFile("anotherTest.file") shouldBe 'exists
  }

  it should "execute Touch file transformation" taggedAs (DriverTests) in {
    outputFile("aTest.file") should not be 'exists

    fileSystemDriver.runAndWait(Touch(outputPath("aTest.file"))) shouldBe a[DriverRunSucceeded[_]]

    outputFile("aTest.file") shouldBe 'exists
  }

  it should "execute Delete file transformations on single files" taggedAs (DriverTests) in {
    createInputFile("aTest.file")
    inputFile("aTest.file") shouldBe 'exists

    fileSystemDriver.runAndWait(Delete(inputPath("aTest.file"))) shouldBe a[DriverRunSucceeded[_]]

    inputFile("aTest.file") should not be 'exists
  }

  it should "execute Delete file transformations on folders recursively" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")

    inputFile(s"subfolder${/}aTest.file") shouldBe 'exists

    fileSystemDriver.runAndWait(Delete(inputPath("subfolder"), true)) shouldBe a[DriverRunSucceeded[_]]

    inputFile("subfolder") should not be 'exists
  }

  it should "execute CopyFrom file transformations by copying a single file to partition path of view" taggedAs (DriverTests) in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out
    }

    createInputFile("aTest.file")
    new File(s"${product.fullPath}${/}aTest.file") should not be 'exists

    fileSystemDriver.runAndWait(CopyFrom(inputPath("*.file"), product, false)) shouldBe a[DriverRunSucceeded[_]]

    new File(s"${product.fullPath}${/}aTest.file") shouldBe 'exists
  }

  it should "execute CopyFrom file transformations by copying a folder recursively to partition path of view" taggedAs (DriverTests) in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out
    }

    createInputFile(s"subfolder${/}aTest.file")
    new File(s"${product.fullPath}${/}aTest.file") should not be 'exists

    fileSystemDriver.runAndWait(CopyFrom(inputPath("subfolder"), product, true)) shouldBe a[DriverRunSucceeded[_]]

    new File(s"${product.fullPath}${/}subfolder${/}aTest.file") shouldBe 'exists
  }

  it should "run asynchronously" taggedAs (DriverTests) in {
    outputFile("aTest.file") should not be 'exists

    val runHandle = fileSystemDriver.run(Touch(outputPath("aTest.file")))

    var runWasAsynchronous = false

    while (fileSystemDriver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[FilesystemTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    fileSystemDriver.getDriverRunState(runHandle) shouldBe a[DriverRunSucceeded[FilesystemTransformation]]

    outputFile("aTest.file") shouldBe 'exists
  }

  it should "return DriverRunFailed in case of problems when running asynchronously" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")
    inputFile("subfolder") shouldBe 'exists

    val runHandle = fileSystemDriver.run(Delete(inputPath("subfolder"), false))

    var runWasAsynchronous = false

    while (fileSystemDriver.getDriverRunState(runHandle).isInstanceOf[DriverRunOngoing[FilesystemTransformation]])
      runWasAsynchronous = true

    runWasAsynchronous shouldBe true
    fileSystemDriver.getDriverRunState(runHandle) shouldBe a[DriverRunFailed[FilesystemTransformation]]

    inputFile("subfolder") shouldBe 'exists
  }

  it should "return DriverRunFailed in case of problems when running synchronously" taggedAs (DriverTests) in {
    createInputFile(s"subfolder${/}aTest.file")
    inputFile("subfolder") shouldBe 'exists

    val runState = fileSystemDriver.runAndWait(Delete(inputPath("subfolder"), false))

    runState shouldBe a[DriverRunFailed[FilesystemTransformation]]

    inputFile("subfolder") shouldBe 'exists
  }

}