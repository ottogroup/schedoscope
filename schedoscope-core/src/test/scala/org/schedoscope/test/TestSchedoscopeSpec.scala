package org.schedoscope.test

import org.apache.hadoop.fs.Path
import org.schedoscope.dsl.Field._
import org.schedoscope.dsl.Parameter._
import test.views.{Click, ClickOfEC0101}


class TestSchedoscopeSpec extends SchedoscopeSpec {

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

  val click = putViewUnderTest {
    new ClickOfEC0101(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      withResource("test" -> "src/test/resources/test.sql")
    }
  }

  //import the view under test to access it in the tests
  import click._

  "the spec" should "produce the right amount of rows" in {
    numRows() shouldBe 3
  }

  it should "contain some events" in {
    row(v(id) shouldBe "event01",
      v(url) shouldBe "http://ec0101.com/url1")
    row(v(id) shouldBe "event02",
      v(url) shouldBe "http://ec0101.com/url2")
  }

  it should "increment the row counter" in {
    startWithRow(2)
    row(v(id) shouldBe "event03",
      v(url) shouldBe "http://ec0101.com/url3")
  }

  it should "load the local resource into hfds" in {
    val fs = click.resources.fileSystem
    val target = new Path(s"${click.resources.remoteTestDirectory}/test.sql")
    fs.exists(target) shouldBe true
  }
}

class TestReusableFixtures extends SchedoscopeSpec with ReusableHiveSchema {

  val ec0101static = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "01"),
      v(url, "url1"))
    set(
      v(id, "02"),
      v(url, "url2"))
    set(
      v(id, "03"),
      v(url, "url3"))
    set(
      v(id, "04"),
      v(url, "url4"))
  }

  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with InputSchema

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with InputSchema

  val click = new ClickOfEC0101(p("2014"), p("01"), p("01")) with OutputSchema {
    basedOn(ec0101Clicks, ec0106Clicks)
    withResource("test" -> "src/test/resources/test.sql")
  }

  val click2 = new ClickOfEC0101(p("2014"), p("01"), p("01")) with OutputSchema {
    basedOn(ec0101static)
    withResource("test" -> "src/test/resources/test.sql")
  }

  import click._

  "the test" should "do" in {
    {
      import ec0101Clicks._
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
    {
      import ec0106Clicks._
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
    then(click)
    numRows() shouldBe 3
  }

  it should "do this" in {

    {
      import ec0101Clicks._
      set(
        v(id, "event01"),
        v(url, "url1"))
      set(
        v(id, "event02"),
        v(url, "url2"))
      set(
        v(id, "event03"),
        v(url, "url3"))
    }
    then(click)
    row(v(id) shouldBe "event01",
      v(url) shouldBe "url1")
    row(v(id) shouldBe "event02",
      v(url) shouldBe "url2")

  }

  it should "handle tests with static input views" in {
    then(click2)
    numRows() shouldBe 4
    row(v(id) shouldBe "01",
      v(url) shouldBe "url1")
    row(v(id) shouldBe "02",
      v(url) shouldBe "url2")
  }

  it should "handle recurring tests with static input views" in {
    then(click2)
    numRows() shouldBe 4
    row(v(id) shouldBe "01",
      v(url) shouldBe "url1")
    row(v(id) shouldBe "02",
      v(url) shouldBe "url2")
  }

  it should "load the local resource into hfds" in {
    val fs = resources.fileSystem
    val target = new Path(s"${resources.remoteTestDirectory}/test.sql")
    fs.exists(target) shouldBe true
  }


}
