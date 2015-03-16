package com.ottogroup.bi.soda.dsl.views

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter.p
import com.ottogroup.bi.soda.dsl.View.t
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.ParsedView
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.parse
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.parseParameters

import test.eci.datahub.Brand
import test.eci.datahub.Product

class ViewUrlParserTest extends FlatSpec with Matchers {

  "ViewUrlParse.parse(viewUrlPath)" should "start with /env/package/view" in {
    val List(ParsedView(env, clazz, arguments)) = parse("dev", "/test.eci.datahub/Brand")
    env shouldBe "dev"
    clazz shouldBe classOf[Brand]
    arguments should be(empty)
  }

  it should "work without preceding /" in {
    val List(ParsedView(env, clazz, arguments)) = parse("dev", "test.eci.datahub/Brand")
    env shouldBe "dev"
    clazz shouldBe classOf[Brand]
    arguments should be(empty)
  }

  it should "work with trailing /" in {
    val List(ParsedView(env, clazz, arguments)) = parse("dev", "test.eci.datahub/Brand/")
    env shouldBe "dev"
    clazz shouldBe classOf[Brand]
    arguments should be(empty)
  }

  it should "work with preceding and trailing /" in {
    val List(ParsedView(env, clazz, arguments)) = parse("dev", "/test.eci.datahub/Brand/")
    env shouldBe "dev"
    clazz shouldBe classOf[Brand]
    arguments should be(empty)
  }

  it should "parse parameters as well" in {
    val List(ParsedView(env, clazz, arguments)) = parse("dev", "/test.eci.datahub/Product/EC0106/2014/01/12/")
    env shouldBe "dev"
    clazz shouldBe classOf[Product]
    arguments should be(List(t(p("EC0106")), t(p("2014")), t(p("01")), t(p("12"))))
  }

  it should "parse multiple views" in {
    val parsedViews = parse("dev", "/test.eci.datahub/e(Product,Brand)/EC0106/2014/01/12/")
    parsedViews.size shouldBe 2
    parsedViews(0).env shouldBe "dev"
    parsedViews(0).viewClass shouldBe classOf[Product]
    parsedViews(0).parameters should be(List(t(p("EC0106")), t(p("2014")), t(p("01")), t(p("12"))))
    parsedViews(1).env shouldBe "dev"
    parsedViews(1).viewClass shouldBe classOf[Brand]
    parsedViews(1).parameters should be(List(t(p("EC0106")), t(p("2014")), t(p("01")), t(p("12"))))
  }

  it should "fail when called with not enough arguments" in {
    an[IllegalArgumentException] should be thrownBy parse("dev", "/test.eci.datahub/")
  }

  it should "fail when called with illegal class name" in {
    an[IllegalArgumentException] should be thrownBy parse("dev", "/test.eci.datahub/Brund/")
  }

  it should "fail when called with illegal package name" in {
    an[IllegalArgumentException] should be thrownBy parse("dev", "/eci.datahub/Brand/")
  }

  "ViewUrlParse.parseParameters(viewUrlPath)" should "parse basic view parameter types" in {
    parseParameters(List("null()")) shouldBe List(List(null))

    val List(List(stringParameter)) = parseParameters(List("Hello"))
    stringParameter.t shouldBe manifest[Parameter[String]]
    stringParameter.v shouldBe p("Hello")

    val List(List(intParameter)) = parseParameters(List("i(2)"))
    intParameter.t shouldBe manifest[Parameter[Int]]
    intParameter.v shouldBe p(2)

    val List(List(longParameter)) = parseParameters(List("l(2)"))
    longParameter.t shouldBe manifest[Parameter[Long]]
    longParameter.v shouldBe p(2l)

    val List(List(byteParameter)) = parseParameters(List("b(2)"))
    byteParameter.t shouldBe manifest[Parameter[Byte]]
    byteParameter.v shouldBe p(2.asInstanceOf[Byte])

    val List(List(doubleParameter)) = parseParameters(List("d(2.1)"))
    doubleParameter.t shouldBe manifest[Parameter[Double]]
    doubleParameter.v shouldEqual p(2.1d)

    val List(List(floatParameter)) = parseParameters(List("f(2.1)"))
    floatParameter.t shouldBe manifest[Parameter[Float]]
    floatParameter.v shouldEqual p(2.1f)

    val List(List(trueParameter)) = parseParameters(List("t(true)"))
    trueParameter.t shouldBe manifest[Parameter[Boolean]]
    trueParameter.v shouldEqual p(true)

    val List(List(falseParameter)) = parseParameters(List("t(false)"))
    falseParameter.t shouldBe manifest[Parameter[Boolean]]
    falseParameter.v shouldEqual p(false)
  }

  it should "consider quoting of reserved characters" in {
    val List(List(stringParameter)) = parseParameters(List("null\\(\\)"))
    stringParameter.t shouldBe manifest[Parameter[String]]
    stringParameter.v shouldBe p("null()")
  }

  it should "support monthly parameterization" in {
    val List(List(year, month)) = parseParameters(List("ym(201412)"))
    year.t shouldBe manifest[Parameter[String]]
    year.v shouldBe p("2014")
    month.t shouldBe manifest[Parameter[String]]
    month.v shouldBe p("12")
  }

  it should "support daily parameterization" in {
    val List(List(year, month, day)) = parseParameters(List("ymd(20141231)"))
    year.t shouldBe manifest[Parameter[String]]
    year.v shouldBe p("2014")
    month.t shouldBe manifest[Parameter[String]]
    month.v shouldBe p("12")
    day.t shouldBe manifest[Parameter[String]]
    day.v shouldBe p("31")
  }

  it should "parse multiple parameters (1)" in {
    val List(List(ecShopCode, year, month, day)) = parseParameters(List("EC0106", "ymd(20141231)"))
    ecShopCode.t shouldBe manifest[Parameter[String]]
    ecShopCode.v shouldBe p("EC0106")
    year.t shouldBe manifest[Parameter[String]]
    year.v shouldBe p("2014")
    month.t shouldBe manifest[Parameter[String]]
    month.v shouldBe p("12")
    day.t shouldBe manifest[Parameter[String]]
    day.v shouldBe p("31")
  }

  it should "parse multiple parameters (2)" in {
    val List(List(ecShopCode, year, month, day)) = parseParameters(List("EC0106", "2014", "01", "20"))
    ecShopCode.t shouldBe manifest[Parameter[String]]
    ecShopCode.v shouldBe p("EC0106")
    year.t shouldBe manifest[Parameter[String]]
    year.v shouldBe p("2014")
    month.t shouldBe manifest[Parameter[String]]
    month.v shouldBe p("01")
    day.t shouldBe manifest[Parameter[String]]
    day.v shouldBe p("20")
  }

  it should "parse monthly date parameter ranges" in {
    val argumentLists = parseParameters(List("rym(201312-201402)"))

    argumentLists should have length 3
    argumentLists.map {
      case List(year, month) => s"${year.v.asInstanceOf[Parameter[String]].v.get}${month.v.asInstanceOf[Parameter[String]].v.get}"
    }.foreach { s =>
      s should be <= "201402"
      s should be >= "201312"
    }
  }

  it should "order date range of monthly date parameter automatically" in {
    val argumentLists = parseParameters(List("rym(201402-201312)"))

    argumentLists should have length 3
    argumentLists.map {
      case List(year, month) => s"${year.v.asInstanceOf[Parameter[String]].v.get}${month.v.asInstanceOf[Parameter[String]].v.get}"
    }.foreach { s =>
      s should be <= "201402"
      s should be >= "201312"
    }
  }

  it should "parse daily date parameter ranges" in {
    val argumentLists = parseParameters(List("rymd(20131202-20140224)"))

    argumentLists should have length 85
    argumentLists.map {
      case List(year, month, day) => s"${year.v.asInstanceOf[Parameter[String]].v.get}${month.v.asInstanceOf[Parameter[String]].v.get}${day.v.asInstanceOf[Parameter[String]].v.get}"
    }.foreach { s =>
      s should be <= "20140224"
      s should be >= "20131202"
    }
  }

  it should "order date range of daily date parameter automatically" in {
    val argumentLists = parseParameters(List("rymd(20140224-20131202)"))

    argumentLists should have length 85
    argumentLists.map {
      case List(year, month, day) => s"${year.v.asInstanceOf[Parameter[String]].v.get}${month.v.asInstanceOf[Parameter[String]].v.get}${day.v.asInstanceOf[Parameter[String]].v.get}"
    }.foreach { s =>
      s should be <= "20140224"
      s should be >= "20131202"
    }
  }

  it should "support non-string enumerations" in {
    val List(List(enumerationValue1), List(enumerationValue2), List(enumerationValue3)) = parseParameters(List("ei(1,2,3)"))

    enumerationValue1 shouldBe t(p(1))
    enumerationValue2 shouldBe t(p(2))
    enumerationValue3 shouldBe t(p(3))
  }

  it should "support string enumerations" in {
    val List(List(enumerationValue1), List(enumerationValue2), List(enumerationValue3)) = parseParameters(List("e(1,2,3)"))

    enumerationValue1 shouldBe t(p("1"))
    enumerationValue2 shouldBe t(p("2"))
    enumerationValue3 shouldBe t(p("3"))
  }

  it should "support range enumerations" in {
    val argumentLists = parseParameters(List("erym(201402-201401,201406-201405)"))

    argumentLists.map {
      case List(year, month) => s"${year.v.asInstanceOf[Parameter[String]].v.get}${month.v.asInstanceOf[Parameter[String]].v.get}"
    }.foreach { s =>
      s should be <= "201406"
      s should be >= "201401"
      s shouldNot be("201403")
      s shouldNot be("201404")
    }

  }

  it should "permutate argument value ranges" in {
    val argumentLists = parseParameters(List("EC0106", "rymd(20140224-20131202)", "rym(201402-201312)", "ei(9,10)"))

    argumentLists should have length 85 * 3 * 2
    argumentLists.foreach {
      case List(ecShopCode, year, month, day, anotherYear, anotherMonth, number) => {
        val dateString = s"${year.v.asInstanceOf[Parameter[String]].v.get}${month.v.asInstanceOf[Parameter[String]].v.get}${day.v.asInstanceOf[Parameter[String]].v.get}"
        val anotherDateString = s"${anotherYear.v.asInstanceOf[Parameter[String]].v.get}${anotherMonth.v.asInstanceOf[Parameter[String]].v.get}"

        dateString should be <= "20140224"
        dateString should be >= "20131202"
        anotherDateString should be <= "201402"
        anotherDateString should be >= "201312"
        ecShopCode shouldBe t(p("EC0106"))
        number should (equal(t(p(9))) or equal(t(p(10))))
      }
    }
  }
}