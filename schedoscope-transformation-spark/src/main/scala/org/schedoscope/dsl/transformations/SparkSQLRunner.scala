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
package org.schedoscope.dsl.transformations

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Implements a simple Spark job that runs the Hive / SparkSQL statement passed as its argument.
  * As no result is returned, this makes only sense for running DDL statements.
  */
object SparkSQLRunner {


  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf())
    val hc = new HiveContext(sc)

    try {

      if (args.length != 1)
        throw new IllegalArgumentException(s"Pass a SQL statement as an argument")

      val statement = args(0)

      println("Executing query:")
      println("================")
      println(statement)

      hc.sql(statement)

    } catch {
      case t: Throwable =>
        t.printStackTrace()
        System.exit(1)
    } finally
      sc.stop()

  }
}