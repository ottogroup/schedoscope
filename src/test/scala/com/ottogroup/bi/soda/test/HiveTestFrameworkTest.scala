package com.ottogroup.bi.soda.test

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter._
import com.ottogroup.bi.soda.dsl.Field._
import test.eci.datahub.Click
import test.eci.datahub.ClickOfEC0101
import com.ottogroup.bi.soda.DriverTests
import test.eci.datahub.ClickOfEC0101

class HiveTestFrameworkTest extends FlatSpec with Matchers {
  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event01"),
      v(url, "http://ec0101.com/url1"))
    set(
      v(id, "event02"),
      v(url, "http://ec0101.com/url2"))
    set(
      v(id, "event03"),
      v(url, "http://ec0101.com/url3"))
  }

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event04"),
      v(url, "http://ec0106.com/url1"))
    set(
      v(id, "event05"),
      v(url, "http://ec0106.com/url2"))
    set(
      v(id, "event06"),
      v(url, "http://ec0106.com/url3"))
  }

  "Hive test framework" should "execute hive transformations locally" taggedAs (DriverTests) in {
    new ClickOfEC0101(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      then()
      numRows shouldBe 3
      row(v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")
    }
  }
}