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
  * Implements a simple Spark job that runs the Hive / SparkSQL statement in the environment variable SQL_RUNNER_HIVE.
  * As no result is printed / returned, this makes only sense for running DDL statements.
  */
object SparkSQLRunner {

  val SQL_STATEMENT = "SQL_RUNNER_HIVE"

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf())
    val hc = new HiveContext(sc)

    try {

      if (System.getenv(SQL_STATEMENT) == null)
        throw new IllegalArgumentException(s"Environment variable $SQL_STATEMENT not set.")

      val statement = System.getenv(SQL_STATEMENT).stripMargin

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

    System.exit(0)
  }
}