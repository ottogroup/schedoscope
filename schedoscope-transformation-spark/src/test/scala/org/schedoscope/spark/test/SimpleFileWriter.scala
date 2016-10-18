package org.schedoscope.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Simple Spark job that writes its arguments, environment, and conf to a file
  */
object SimpleFileWriter {

  // We want the configuration to be overridable in tests.
  var confBuilder = () => new SparkConf()

  def main(args: Array[String]) {

    if (args.length < 1)
      throw new IllegalArgumentException("The first parameter must be the output file path")

    val outPath: String = args.head

    val sc = new SparkContext(confBuilder())
    sc.setLogLevel("ERROR")

    try {

      val output: Array[(String, String)] =
        args.map(("arg", _)) ++
          System.getenv().map { case (k, v) => (k.toString, v.toString) } ++
          sc.getConf.getAll

      val rdd: RDD[String] = sc.parallelize(output).map { case (k,v) => s"$k\t$v" }

      rdd.saveAsTextFile(outPath)

    } finally
      sc.stop()
  }
}
