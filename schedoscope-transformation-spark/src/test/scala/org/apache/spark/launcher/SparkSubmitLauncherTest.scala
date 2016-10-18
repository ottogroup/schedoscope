package org.apache.spark.launcher

import java.io.File

import scala.collection.JavaConversions._
import org.apache.commons.io.FileUtils
import org.apache.spark.launcher.SparkAppHandle.State._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.dsl.transformations.SparkTransformation._
import org.schedoscope.spark.test.SimpleFileWriter


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
    launcher.setMainClass(nameOf(SimpleFileWriter))
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
