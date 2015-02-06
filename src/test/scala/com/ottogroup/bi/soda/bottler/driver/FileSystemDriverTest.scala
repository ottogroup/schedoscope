package com.ottogroup.bi.soda.bottler.driver

import org.scalatest.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import com.ottogroup.bi.soda.dsl.transformations.filesystem._
import java.io.File

class FileSystemDriverTest extends FlatSpec with BeforeAndAfter with Matchers with TestFolder {
  val driver = new FileSystemDriver(UserGroupInformation.getLoginUser(), new Configuration())

  "Copy" should "copy data into target directory" in {

    assert(driver.runAndWait(new Copy(in.getAbsolutePath() + "/*", out.getAbsolutePath(), false)) == true)

    assert(new File(out, "test.txt").exists() == true)
    assert(new File(out, "subdir/test2.txt").exists() == false)

    // source exists, destination not writable
    // source exists, destination writable      
  }
  "Copy(recursive=true)" should "copy data and subdirectories into target directory" in {

    assert(driver.runAndWait(new Copy(in.getAbsolutePath() + "/*", out.getAbsolutePath(), true)) == true) // source exists, destination writable
    assert(new File(out, "test.txt").exists())
    assert(new File(out, "subdir/test2.txt").exists())
    // source exists, destination not writable
    // source exists, destination writable      
  }

  "Move" should "move data into target directory" in {
    assert(driver.runAndWait(new Move(in.getAbsolutePath() + "/test.txt", out.getAbsolutePath())) == true) // source exists, destination writable
    assert(new File(out, "test.txt").exists())
    assert(!new File(in, "test.txt").exists())
  }
  "Delete" should "delete data in target directory" in {

    assert(driver.runAndWait(new Delete(out.getAbsolutePath() + "/test.txt")) == true) // source exists, destination writable
    assert(!new File(out, "test.txt").exists())

  }
  "Touch" should "create file" in {

    assert(driver.runAndWait(new Touch(out.getAbsolutePath() + "/_SUCCESS")) == true) // source exists, destination writable
    assert(new File(out, "_SUCCESS").exists())
  }

  "IfExists" should "copy data into target directory" in {
  }

  "IfNotExists" should "copy data into target directory" in {
  }

}