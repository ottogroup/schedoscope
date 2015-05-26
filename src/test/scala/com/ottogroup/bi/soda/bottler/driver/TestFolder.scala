package com.ottogroup.bi.soda.bottler.driver

import java.io.File
import org.scalatest._
import org.apache.commons.io.FileUtils

trait TestFolder extends AbstractSuite { self: Suite =>
  var testFolder: File = _
  var in: File = _
  var out: File = _

  private def deleteFile(file: File) {
    if (!file.exists) return
    if (file.isFile) {
      file.delete()
    } else {
      file.listFiles().foreach(deleteFile)
      file.delete()
    }
  }

  abstract override def withFixture(test: NoArgTest) = {

    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(tempFolder, "scalatest-" + System.nanoTime)
    } while (!folder.mkdir())

    testFolder = folder

    in = new File(testFolder, "in");
    in.mkdir()
    out = new File(testFolder, "out")

    FileUtils.copyURLToFile(getClass().getResource("/input/test.txt"), new File(in, "test.txt"))
    val in2 = new File(in, "subdir")
    in2.mkdir()
    FileUtils.copyURLToFile(getClass().getResource("/input/test.txt"), new File(in2, "test2.txt"))

    out.mkdir()
    try {
      super.withFixture(test)
    } finally {
      deleteFile(testFolder)
    }
  }
}

