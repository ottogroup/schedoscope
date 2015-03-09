package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import com.ottogroup.bi.soda.dsl.transformations.filesystem._
import java.io.File
import test.eci.datahub.Product
import com.ottogroup.bi.soda.dsl.Parameter._

class FileSystemDriverTest extends FlatSpec with BeforeAndAfter with Matchers with TestFolder {
  val driver = new FileSystemDriver(UserGroupInformation.getLoginUser(), new Configuration())

  "Copy" should "copy files into target directory" in {
    driver.runAndWait(Copy(in.getAbsolutePath() + File.separator + "*", out.getAbsolutePath(), false)) shouldBe a [DriverRunSucceeded[_]]

    new File(out, "test.txt") shouldBe 'exists
    new File(out, "subdir" + File.separator + "test2.txt") should not be 'exists
  }

  it should "copy classpath resources into target directory" in {
    driver.runAndWait(Copy("classpath://input/classpathtest.txt", out.getAbsolutePath(), false)) shouldBe a [DriverRunSucceeded[_]]
    new File(out, "classpathtest.txt") shouldBe 'exists
  }

  "Copy(recursive=true)" should "copy files and subdirectories into target directory" in {
    driver.runAndWait(Copy(in.getAbsolutePath() + File.separator + "*", out.getAbsolutePath(), true)) shouldBe a [DriverRunSucceeded[_]]
    new File(out, "test.txt") shouldBe 'exists
    new File(out, "subdir/test2.txt") shouldBe 'exists
  }

  "Move" should "move files into target directory" in {
    driver.runAndWait(Move(in.getAbsolutePath() + File.separator + "test.txt", out.getAbsolutePath())) shouldBe a [DriverRunSucceeded[_]]
    new File(out, "test.txt") shouldBe 'exists
    new File(in, "test.txt") should not be 'exists
  }

  "Delete" should "delete files in target directory" in {
    driver.runAndWait(Delete(out.getAbsolutePath() + File.separator + "test.txt")) shouldBe a [DriverRunSucceeded[_]]
    new File(out, "test.txt") should not be 'exists
  }

  "Touch" should "create file" in {
    driver.runAndWait(Touch(out.getAbsolutePath() + File.separator + "_SUCCESS")) shouldBe a [DriverRunSucceeded[_]]
    new File(out, "_SUCCESS") shouldBe 'exists
  }

  "CopyFrom" should "copy data into view instance directory" in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out.toString()
    }

    driver.runAndWait(CopyFrom(in.getAbsolutePath() + File.separator + "*", product, false)) shouldBe a [DriverRunSucceeded[_]]
    new File(out, "test.txt") shouldBe 'exists
  }
}