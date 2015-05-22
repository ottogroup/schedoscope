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

import org.apache.commons.io.FileUtils
import org.scalatest.AbstractSuite
import org.scalatest.Suite

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

