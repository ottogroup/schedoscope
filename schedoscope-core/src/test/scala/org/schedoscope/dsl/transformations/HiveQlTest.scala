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

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.schedoscope.dsl.{Parameter, Structure, View}

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

  "the normalize query function" should "delete comments" in {
    val qry = "-----23123-xcvo\tn .cvaotrsaooiarst9210132q56`]-[ taroistneaorsitdthdastrtsra\n" +
      "--a comment\n" +
      //      "--no comment\n" +
      "select * from coolstuff\n" +
      "LIMIT 10"

    HiveTransformation.normalizeQuery(qry) shouldBe "select * from coolstuff LIMIT 10"
  }

  it should "remove whitespaces from the start and end of lines" in {
    val qry = "   \t\t\t    test \t \n" +
      "  \t \tte\tst    \n" +
      "wh at\t   "

    HiveTransformation.normalizeQuery(qry) shouldBe "test te st wh at"
  }

  it should "remove set commands" in {
    val qry = "NOSET key=value;\n" +
      "SET test=hello; \n" +
      "SELECT;"

    HiveTransformation.normalizeQuery(qry) shouldBe "NOSET key=value; SELECT;"
  }

  it should "remove duplicate whitespaces" in {
    val qry1 = "\"this  is\"   a\ttest"
    val qry2 = "'this  is'   a\ttest"
    val qry3 = "'  '  '  ' "
    val qry4 = "'  \\'  '  '  \\'  ' "

    HiveTransformation.normalizeQuery(qry1) shouldBe "\"this;;is\" a test"
    HiveTransformation.normalizeQuery(qry2) shouldBe "'this;;is' a test"
    HiveTransformation.normalizeQuery(qry3) shouldBe "';;' ';;'"
    HiveTransformation.normalizeQuery(qry4) shouldBe "';;\\';;' ';;\\';;'"
  }

  it should "normalize two queries to be equal" in {
    val qry1 = "SET memory=1024;\n" +
      "SET date='20160412';\n" +
      "\n" +
      "SELECT \n" +
      "  price \n" +
      "FROM \n" +
      "  transactions\n" +
      "WHERE\n" +
      "  date='${date}'"

    val qry2 = "SET memory=2028;\n" +
      "SET date='20160413';\n" +
      "\n" +
      "\n" +
      "SELECT price \n" +
      "FROM transactions\n" +
      "WHERE date='${date}'"

    HiveTransformation.normalizeQuery(qry1) should
      equal(HiveTransformation.normalizeQuery(qry2))
  }

  "replace Whitespaces in quotes" should "do it's thing" in {
    val replaceDQ = HiveTransformation.replaceWhitespacesBetweenChars("\"") _
    replaceDQ("\"") shouldBe "\""
    replaceDQ("\"\"") shouldBe "\"\""
    replaceDQ("test") shouldBe "test"
    replaceDQ("te\"st") shouldBe "te\"st"
    replaceDQ("\"  \"") shouldBe "\";;\""
    replaceDQ("\"  \"\\\"") shouldBe "\";;\"\\\""
    replaceDQ("\"  \" \" \"") shouldBe "\";;\" \";\""
    replaceDQ("hi\"  \" \" \"") shouldBe "hi\";;\" \";\""
    replaceDQ("hi\"  \" test \" \"hi") shouldBe "hi\";;\" test \";\"hi"
    replaceDQ("\" te\\\"st \"") shouldBe "\";te\\\"st;\""
    replaceDQ("\" te\\\"st\\\" \"") shouldBe "\";te\\\"st\\\";\""
    replaceDQ("\" \\\"") shouldBe "\" \\\""
    replaceDQ("\\\" \"") shouldBe "\\\" \""
    replaceDQ("\" \\\"s") shouldBe "\" \\\"s"
    replaceDQ("\\\" \"s") shouldBe "\\\" \"s"

  }

  it should "work when nested" in {
    val replaceDQ = HiveTransformation.replaceWhitespacesBetweenChars("\"") _
    val replaceSQ = HiveTransformation.replaceWhitespacesBetweenChars("'") _

    val replace = (s: String) => replaceSQ(replaceDQ(s))

    replace("\"  \"") shouldBe "\";;\""
    replace("\"  \"\\\"") shouldBe "\";;\"\\\""
    replace("\"  \" ' '") shouldBe "\";;\" ';'"
    replace("hi\"  \" \" \"") shouldBe "hi\";;\" \";\""
    replace("\" \' \" \'") shouldBe "\";';\";'"
    replace("\" \' \' \"") shouldBe "\";\';\';\""
    replace("\" \\' \" \\'") shouldBe "\";\\';\" \\'"
  }

  "Hivetransformation checksum" should "apply the normalize function" in {

    val settings = Map(
      "memory" -> "12",
      "date" -> "20160102")

    val orderAll = OrderAll(p(2014), p(10), p(12))

    val hiveTransformation = new HiveTransformation(
      HiveTransformation.insertInto(
        orderAll,
        "select * from\t\nprices  where id = '12  23'",
        false,
        settings)
      , List()
    )

    hiveTransformation.stringsToChecksum shouldBe
      List("INSERT OVERWRITE TABLE dev_org_schedoscope_dsl_transformations.order_all " +
        "select * from prices where id = '12;;23'")
  }

  "Hivetransformation.validateQuery" should "find uneven amount of joins and ons" in {
    val qry1 = "SELECT JOIN ON TEST"
    val qry2 = "SELECT join on TEST"
    val qry3 = "SELECT 'join' on JOIN"
    val qry4 = "JOIN ON"
    val qry5 = "ON"
    val qry6 = "JOIN"
    val qry7 = "JOIN ON ON"
    val qry8 = "JOIN JOIN ON ON"
    val qry9 = "SELECT \"join\" on JOIN"
    val qry10 = "SELECT \\\"join\" on JOIN"

    HiveTransformation.compareJoinsOns(qry1) shouldBe true
    HiveTransformation.compareJoinsOns(qry2) shouldBe true
    HiveTransformation.compareJoinsOns(qry3) shouldBe true
    HiveTransformation.compareJoinsOns(qry4) shouldBe true
    HiveTransformation.compareJoinsOns(qry5) shouldBe false
    HiveTransformation.compareJoinsOns(qry6) shouldBe false
    HiveTransformation.compareJoinsOns(qry7) shouldBe false
    HiveTransformation.compareJoinsOns(qry8) shouldBe true
    HiveTransformation.compareJoinsOns(qry9) shouldBe true
    HiveTransformation.compareJoinsOns(qry10) shouldBe false
  }

}
