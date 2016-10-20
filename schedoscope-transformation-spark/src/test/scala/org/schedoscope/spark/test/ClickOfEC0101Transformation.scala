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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Implements the transformation for ClickOfEC0101 test with Spark SQL
  */
object ClickOfEC0101Transformation {

  // We want the configuration to be overridable in tests.
  var confBuilder: () => SparkConf = () => new SparkConf()

  def main(args: Array[String]) {

    if (args.length < 7)
      throw new IllegalArgumentException("Args: <sourceTable> <targetTable> <shopCode> <year> <month> <day> <dateId>")

    val sourceTable = args(0)
    val targetTable = args(1)
    val shopCode = args(2)
    val year = args(3)
    val month = args(4)
    val day = args(5)
    val dateId = args(6)

    val sc = new SparkContext(confBuilder())
    val hc = new HiveContext(sc)

    val query =
      s"""
         |INSERT INTO TABLE ${targetTable} PARTITION(year = '$year', month = '$month', day = '$day', date_id = '$dateId')
         |SELECT *
         |FROM ${sourceTable}
         |WHERE shop_code = '$shopCode' AND  year = '$year' AND month = '$month' AND day = '$day' AND date_id = '$dateId'
         """.stripMargin

    try {

      hc.sql(query)

    } catch {
      case t: Throwable => t.printStackTrace()
        System.exit(1)
    } finally
      sc.stop()

    System.exit(0)
  }
}