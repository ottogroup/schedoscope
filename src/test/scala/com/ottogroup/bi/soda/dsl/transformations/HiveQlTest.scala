package com.ottogroup.bi.soda.dsl.transformations

import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import java.util.Date
import org.jooq.SQLDialect
import org.jooq.impl.DSL._
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Structure
import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter._
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQl
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQl._
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQlDsl._

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
  val id = fieldOf[Int]
  val date = fieldOf[Date]
  val customerNumber = fieldOf[String]
}

case class OrderAll(year: Parameter[Int], month: Parameter[Int], day: Parameter[Int]) extends View {
  val id = fieldOf[Int]
  val date = fieldOf[Date]
  val customerNumber = fieldOf[String]
  val pos = fieldOf[Int]
  val article = fieldOf[String]
  val number = fieldOf[Int]
  val price = fieldOf[Float]

  val orderItem = dependsOn(() => OrderItem(year, month, day))
  val order = dependsOn(() => Order(year, month, day))

  transformVia(() =>
    HiveQl(HiveQl.insertInto(
      this,
      dsl {
        _.select(order().id, get(orderItem().eans, 0), orderItem().article)
          .from(order())
          .join(orderItem())
          .on(order().id.equal(orderItem().orderId))
          .where(order().id.equal(4711))
      })))
}

class HiveQlTest extends FlatSpec with BeforeAndAfter with Matchers {

  "HiveQl.insertInto" should "generate correct static partitioning prefix by default" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveQl.insertInto(orderAll, "SELECT * FROM STUFF") shouldEqual """INSERT OVERWRITE TABLE dev_com_ottogroup_bi_soda_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * FROM STUFF"""
  }

  it should "not generate a partitioning prefix if requested" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveQl.insertInto(
      orderAll,
      "SELECT * FROM STUFF",
      partition = false) shouldEqual """INSERT OVERWRITE TABLE dev_com_ottogroup_bi_soda_dsl_transformations.order_all
SELECT * FROM STUFF"""
  }

  it should "generate settings if needed" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveQl.insertInto(
      orderAll,
      "SELECT * FROM STUFF",
      settings = Map(
        "parquet.compression" -> "GZIP",
        "my.setting" -> "true")) shouldEqual """SET parquet.compression=GZIP;
SET my.setting=true;
INSERT OVERWRITE TABLE dev_com_ottogroup_bi_soda_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * FROM STUFF"""
  }

  "HiveQl.insertDynamicallyInto" should "generate correct dynamic partitioning prefix by default" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    HiveQl.insertDynamicallyInto(
      orderAll,
      "SELECT * FROM STUFF") shouldEqual """SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE dev_com_ottogroup_bi_soda_dsl_transformations.order_all
PARTITION (year, month, day)
SELECT * FROM STUFF"""
  }

  "HiveQl.replaceParameters" should "replace parameter parameter placeholders" in {
    HiveQl.replaceParameters("${a} ${a} ${b}", Map("a" -> "A", "b" -> Boolean.box(true))) shouldEqual ("A A true")
  }

  "HiveQl.queryFrom" should "read queries from external file" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))
    HiveQl.insertInto(
      orderAll,
      replaceParameters(
        queryFromResource("test.sql"),
        Map(
          "param" -> Int.box(2),
          "anotherParam" -> "Value")),
      settings = Map(
        "parquet.compression" -> "GZIP",
        "my.setting" -> "true")) shouldEqual """SET parquet.compression=GZIP;
SET my.setting=true;
INSERT OVERWRITE TABLE dev_com_ottogroup_bi_soda_dsl_transformations.order_all
PARTITION (year = '2014', month = '10', day = '12')
SELECT * 
FROM STUFF
WHERE param = 2
AND anotherParam = 'Value'"""
  }

  "HiveQlDsl.dsl" should "allow the typesafe specification of a query" in {
    val orderAll = OrderAll(p(2014), p(10), p(12))

    val t = orderAll.transformation().asInstanceOf[HiveQl]
  }
}