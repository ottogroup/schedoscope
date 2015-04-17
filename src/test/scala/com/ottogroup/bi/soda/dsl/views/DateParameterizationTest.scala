package com.ottogroup.bi.soda.dsl.views

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.ottogroup.bi.soda.dsl.Parameter.p
import java.util.Calendar

class DailyParameterizationTest extends FlatSpec with Matchers {
  "prevDay" should "compute the previous date" in {
    val prevDate = DateParameterizationUtils.prevDay(p("2014"), p("01"), p("01")) match {
      case Some((prevYear, prevMonth, prevDay)) => (prevYear, prevMonth, prevDay)
      case None => null
    }

    prevDate shouldEqual ("2013", "12", "31")
  }

  it should "stop at the earliest date" in {
    val prevDate = DateParameterizationUtils.prevDay(p("2013"), p("12"), p("01"))

    prevDate shouldBe None
  }

  "prevMonth" should "compute the previous month" in {
    val prevDate = DateParameterizationUtils.prevMonth(p("2014"), p("01")) match {
      case Some((prevYear, prevMonth)) => (prevYear, prevMonth)
      case None => null
    }

    prevDate shouldEqual ("2013", "12")
  }

  it should "stop at the earliest date" in {
    val prevDate = DateParameterizationUtils.prevMonth(p("2013"), p("12"))

    prevDate shouldBe None
  }

  "thisAndPrevDays" should "deliver all days from the start date down to the earliest date" in {
    val days = DateParameterizationUtils.thisAndPrevDays(p("2014"), p("01"), p("11"))

    var firstDay: String = null
    var lastDay: String = null

    for (day <- days) {
      if (firstDay == null)
        firstDay = s"${day._1}${day._2}${day._3}"
      lastDay = s"${day._1}${day._2}${day._3}"
    }

    firstDay shouldEqual "20140111"
    lastDay shouldEqual "20131201"
  }

  "thisMonthAndPrevDay" should "count down from the last day of the given month" in {
    DateParameterizationUtils.thisAndPrevDays(p("2014"), p("02")) shouldEqual DateParameterizationUtils.thisAndPrevDays(p("2014"), p("02"), p("28"))
  }
  
  "lastDays" should "deliver all days between the current date and the specified one" in {
	  import DateParameterizationUtils._
	  val fromThisDay = Calendar.getInstance()
	  fromThisDay.set(2014, Calendar.OCTOBER, 15)
	  val toThisDay = Calendar.getInstance()
	  toThisDay.set(2014,Calendar.NOVEMBER,14)
      val days = dayParameterRange(dayRange(fromThisDay, toThisDay))
      println(days.toList)
      days.size shouldEqual 31
      days.head shouldEqual ("2014","11","14")
	  days.reverse.head shouldEqual ("2014","10","15")
  }
  "lastMonths" should "deliver all month between the current date and the specified one" in {
	  import DateParameterizationUtils._
	  val fromThisDay = Calendar.getInstance()
	  fromThisDay.set(2013, Calendar.OCTOBER, 1)
	  val toThisDay = Calendar.getInstance()
	  toThisDay.set(2014,Calendar.NOVEMBER,16)
      val days = monthParameterRange(dayRange(fromThisDay, toThisDay))
      println(days.toList)
      days.size shouldEqual 13
      days.head shouldEqual ("2014","11")
	  days.reverse.head shouldEqual ("2013","10")

  }
}