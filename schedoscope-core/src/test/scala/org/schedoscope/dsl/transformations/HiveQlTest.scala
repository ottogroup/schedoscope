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

import java.util.Date

import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.{ Parameter, Structure, View }
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import org.schedoscope.dsl.transformations.Transformation.replaceParameters

case class Article() extends Structure {
  val name = fieldOf[String]
  val number = fieldOf[Int]
}

case class OrderItem(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val orderId = fieldOf[Int]
  val pos = fieldOf[Int]
  val article = fieldOf[Article]
  val price = fieldOf[Float]
  val eans = fieldOf[List[String]]
}

case class Order(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val viewId = fieldOf[Int]
  val date = fieldOf[Date]
  val customerNumber = fieldOf[String]
}

case class OrderAll(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val viewId = fieldOf[Int]
  val date = fieldOf[Date]
  val customerNumber = fieldOf[String]
  val pos = fieldOf[Int]
  val article = fieldOf[String]
  val number = fieldOf[Int]
  val price = fieldOf[Float]

  val orderItem = dependsOn(() => OrderItem(year, month, day))
  val order = dependsOn(() => Order(year, month, day))
}

class HiveTransformationTest extends FlatSpec with BeforeAndAfter with Matchers {

  "HiveTransformation.insertInto" should "generate correct static partitioning prefix by default" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertInto(orderAll, "SELECT * FROM STUFF") shouldEqual
      """INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * FROM STUFF"""
  }

  it should "not generate a partitioning prefix if requested" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertInto(
      orderAll,
      "SELECT * FROM STUFF",
      partition = false) shouldEqual
      """INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
SELECT * FROM STUFF"""
  }

  it should "generate settings if needed" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertInto(
      orderAll,
      "SELECT * FROM STUFF",
      settings = Map(
        "parquet.compression" -> "GZIP",
        "my.setting" -> "true")) shouldEqual
      """SET parquet.compression=GZIP;
SET my.setting=true;
INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * FROM STUFF"""
  }

  "HiveTransformation.insertDynamicallyInto" should "generate correct dynamic partitioning prefix by default" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveTransformation.insertDynamicallyInto(
      orderAll,
      "SELECT * FROM STUFF") shouldEqual
      """SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year, month, day)
SELECT * FROM STUFF"""
  }

  "HiveTransformation.replaceParameters" should "replace parameter parameter placeholders" in {
    replaceParameters("${a} ${a} ${b}", Map("a" -> "A", "b" -> Boolean.box(true))) shouldEqual ("A A true")
  }

  "HiveTransformation.queryFrom" should "read queries from external file" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))
    HiveTransformation.insertInto(
      orderAll,
      replaceParameters(
        queryFromResource("test.sql"),
        Map(
          "param" -> Int.box(2),
          "anotherParam" -> "Value")),
      settings = Map(
        "parquet.compression" -> "GZIP",
        "my.setting" -> "true")) shouldEqual
      """SET parquet.compression=GZIP;
SET my.setting=true;
INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT *
FROM STUFF
WHERE param = 2
AND anotherParam = 'Value'"""
  }
}
