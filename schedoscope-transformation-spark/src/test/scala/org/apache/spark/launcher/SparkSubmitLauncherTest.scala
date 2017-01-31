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
package org.apache.spark.launcher

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.launcher.SparkAppHandle.State._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.dsl.transformations.SparkTransformation._
import org.schedoscope.spark.test.SimpleFileWriter

import scala.collection.JavaConversions._


class SparkSubmitLauncherTest extends FlatSpec with BeforeAndAfter with Matchers {
  var outpath: String = null

  before {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(tempFolder, "sparktest-" + System.nanoTime)
    } while (!folder.mkdir())

    outpath = folder.toString

    FileUtils.deleteDirectory(new File(outpath))
  }


  after {
    FileUtils.deleteDirectory(new File(outpath))
  }

  "SparkSubmitLauncher" should "run Spark jobs in tests without needing a Spark environment" in {

    val launcher = new SparkSubmitLauncher()
    launcher.setAppName("SimpleFileWriter")
    launcher.setMainClass(classNameOf(SimpleFileWriter))
    launcher.setAppResource(jarOf(SimpleFileWriter))
    launcher.addAppArgs(outpath, "one argument", "another argument")
    launcher.setLocalTestMode()

    val handle = launcher.startApplication()

    while (!(handle.getState == FINISHED || handle.getState == FAILED || handle.getState == KILLED))
      Thread.sleep(10)

    val output: String = FileUtils.readLines(new File(outpath + File.separator + "part-00000")).mkString("\n")

    output should include("one argument")
    output should include("another argument")
    output should include("local")
    output should include("SimpleFileWriter")
  }
}
