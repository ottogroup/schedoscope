package com.ottogroup.bi.soda.bottler.driver

import java.io.File
import org.scalatest._
import org.apache.commons.io.FileUtils
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation
import java.net.URL

trait TestFolder extends AbstractSuite { self: Suite =>
  var testFolder: File = _
  var inputFolder: File = _
  var outputFolder: File = _

  def in = inputFolder.getAbsolutePath()
  def out = outputFolder.getAbsolutePath()

  private def deleteFile(file: File) {
    if (!file.exists) return
    if (file.isFile) {
      file.delete()
    } else {
      file.listFiles().foreach(deleteFile)
      file.delete()
    }
  }

  def /() = File.separator

  def createInputFile(path: String) {
    FileUtils.touch(new File(s"${inputFolder}${File.separator}${path}"))
  }

  def outputFile(path: String) = new File(outputPath(path))
  def inputFile(path: String) = new File(inputPath(path))

  def inputPath(path: String) = s"${in}${File.separator}${path}"
  def outputPath(path: String) = s"${out}${File.separator}${path}"

  abstract override def withFixture(test: NoArgTest) = {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(tempFolder, "scalatest-" + System.nanoTime)
    } while (!folder.mkdir())

    testFolder = folder

    inputFolder = new File(testFolder, "in");
    inputFolder.mkdir()
    outputFolder = new File(testFolder, "out")
    outputFolder.mkdir()

    try {
      super.withFixture(test)
    } finally {
      deleteFile(testFolder)
    }
  }
}

