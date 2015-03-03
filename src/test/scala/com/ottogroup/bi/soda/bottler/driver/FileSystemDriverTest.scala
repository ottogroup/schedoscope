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
    assert(driver.runAndWait(Copy(in.getAbsolutePath() + File.separator + "*", out.getAbsolutePath(), false)) == true)

    assert(new File(out, "test.txt").exists())
    assert(!new File(out, "subdir" + File.separator + "test2.txt").exists())
  }
  
  it should "copy classpath resources into target directory" in {
    assert(driver.runAndWait(Copy("classpath://input/classpathtest.txt", out.getAbsolutePath(), false)))
    assert(new File(out, "classpathtest.txt").exists())
  }
  
  "Copy(recursive=true)" should "copy files and subdirectories into target directory" in {
    assert(driver.runAndWait(Copy(in.getAbsolutePath() + File.separator + "*", out.getAbsolutePath(), true)) == true) // source exists, destination writable
    assert(new File(out, "test.txt").exists())
    assert(new File(out, "subdir/test2.txt").exists())
  }

  "Move" should "move files into target directory" in {
    assert(driver.runAndWait(Move(in.getAbsolutePath() + File.separator + "test.txt", out.getAbsolutePath()))) // source exists, destination writable
    assert(new File(out, "test.txt").exists())
    assert(!new File(in, "test.txt").exists())
  }
  
  "Delete" should "delete files in target directory" in {
    assert(driver.runAndWait(Delete(out.getAbsolutePath() + File.separator + "test.txt"))) // source exists, destination writable
    assert(!new File(out, "test.txt").exists())
  }
  
  "Touch" should "create file" in {
    assert(driver.runAndWait(Touch(out.getAbsolutePath() + File.separator + "_SUCCESS"))) // source exists, destination writable
    assert(new File(out, "_SUCCESS").exists())
  }

  "IfExists" should "copy data into target directory" in {
  }

  "IfNotExists" should "copy data into target directory" in {
  }

  "CopyFrom" should "copy data into view instance directory" in {
    val product = new Product(p("EC0106"), p("2014"), p("01"), p("01")) {
      override def fullPath = out.toString()
    }
    
    assert(driver.runAndWait(CopyFrom(in.getAbsolutePath() + File.separator + "*", product, false)))
    assert(new File(out, "test.txt").exists())  
  }
}