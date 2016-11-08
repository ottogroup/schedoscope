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
package org.schedoscope.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Simple Spark job that writes its arguments, environment, and conf to a file
  */
object SimpleFileWriter {

  // We want the configuration to be overridable in tests.
  var confBuilder: () => SparkConf = () => new SparkConf()

  def main(args: Array[String]) {

    if (args.length < 1)
      throw new IllegalArgumentException("The first parameter must be the output file path")

    val outPath: String = args.head

    val sc = new SparkContext(confBuilder())

    try {

      val output: Array[(String, String)] =
        args.map(("arg", _)) ++
          System.getenv().map { case (k, v) => (k.toString, v.toString) } ++
          sc.getConf.getAll

      val rdd: RDD[String] = sc.parallelize(output).map { case (k, v) => s"$k\t$v" }

      rdd.saveAsTextFile(outPath)

    } catch {
      case t: Throwable => t.printStackTrace()
        System.exit(1)
    } finally
      sc.stop()
  }
}
