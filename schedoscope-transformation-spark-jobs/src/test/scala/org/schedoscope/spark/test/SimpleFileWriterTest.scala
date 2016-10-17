package org.schedoscope.spark.test

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConversions._

class SimpleFileWriterTest extends FlatSpec with BeforeAndAfter with Matchers {

  var outpath: String = null

  before {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(tempFolder, "sparktest-" + System.nanoTime)
    } while (!folder.mkdir())

    outpath = folder.toString

    FileUtils.deleteDirectory(new File(outpath))

    SimpleFileWriter.confBuilder = () => new SparkConf().setMaster("local").setAppName("SimpleFileWriter")
  }



  after {
    FileUtils.deleteDirectory(new File(outpath))
  }

  "SimpleFileWriter" should "write its calling arguments, Spark conf, and environment to file" in {

    SimpleFileWriter.main(Array(outpath, "one argument", "another argument"))

    val output: String = FileUtils.readLines(new File(outpath + File.separator + "part-00000")).mkString("\n")

    output should include("one argument")
    output should include("another argument")
    output should include("local")
    output should include("SimpleFileWriter")
  }

}
