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
package org.schedoscope.dsl.views

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.dsl.Parameter.p
import java.util.Calendar

class DailyParameterizationTest extends FlatSpec with Matchers {
  "prevDay" should "compute the previous date" in {
    val prevDate = DateParameterizationUtils.prevDay(p("2014"), p("01"), p("01")) match {
      case Some((prevYear, prevMonth, prevDay)) => (prevYear, prevMonth, prevDay)
      case None                                 => null
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
      case None                        => null
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
    val fromThisDay = parametersToDay(p("2014"), p("10"), p("15"))
    val toThisDay = parametersToDay(p("2014"), p("11"), p("14"))

    val days = dayParameterRange(dayRange(fromThisDay, toThisDay))

    days.size shouldEqual 31
    days.head shouldEqual ("2014", "11", "14")
    days.reverse.head shouldEqual ("2014", "10", "15")
  }

  "lastMonths" should "deliver all months between the current date and the specified one" in {
    import DateParameterizationUtils._
    val fromThisDay = parametersToDay(p("2014"), p("01"), p("11"))
    val toThisDay = parametersToDay(p("2014"), p("11"), p("16"))

    val months = monthParameterRange(dayRange(fromThisDay, toThisDay))

    months.size shouldEqual 11
    months.head shouldEqual ("2014", "11")
    months.reverse.head shouldEqual ("2014", "01")

  }

  "allDaysOfMonth" should "return all days of a month" in {
    val days = DateParameterizationUtils.allDaysOfMonth(p("2014"), p("02"))

    var firstDay: String = null
    var lastDay: String = null

    for (day <- days) {
      if (firstDay == null)
        firstDay = s"${day._1}${day._2}${day._3}"
      lastDay = s"${day._1}${day._2}${day._3}"
    }

    firstDay shouldEqual "20140228"
    lastDay shouldEqual "20140201"
  }
}