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

import java.util.Calendar

import org.schedoscope.Schedoscope
import org.schedoscope.dsl.Parameter
import org.schedoscope.dsl.Parameter.p

import scala.collection.mutable.ListBuffer

object DateParameterizationUtils {
  /**
    * The earliest day in history, as configured. Defaults to 2013/12/01.
    */
  def earliestDay = Schedoscope.settings.earliestDay

  /**
    * Creates a Calendar out of year, month, day encoded as string parameters.
    */
  def parametersToDay(year: Parameter[String], month: Parameter[String], day: Parameter[String]) = {
    val date = Calendar.getInstance()
    date.clear()
    date.set(year.v.get.toInt, month.v.get.toInt - 1, day.v.get.toInt)

    date
  }

  /**
    * Returns a string triple (year, month, day) from a Calendar.
    */
  def dayToStrings(thisDay: Calendar) = {
    val year = s"${"%04d".format(thisDay.get(Calendar.YEAR))}"
    val month = s"${"%02d".format(thisDay.get(Calendar.MONTH) + 1)}"
    val day = s"${"%02d".format(thisDay.get(Calendar.DAY_OF_MONTH))}"

    (year, month, day)
  }

  /**
    * Returns a string parameter triple (year, month, day) from a Calendar.
    */
  def dayToParameters(thisDay: Calendar) = {
    val year: Parameter[String] = p(s"${"%04d".format(thisDay.get(Calendar.YEAR))}")
    val month: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.MONTH) + 1)}")
    val day: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.DAY_OF_MONTH))}")

    (year, month, day)
  }

  /**
    * The latest day in history, either now or the configured latest day, as a string parameter triple (year, month, day).
    */
  def today = dayToParameters(Schedoscope.settings.latestDay)

  /**
    * Given a Calendar, return the previous day as a Calendar or None, if the earliest day is crossed.
    */
  def prevDay(thisDay: Calendar): Option[Calendar] = {
    if (thisDay.after(earliestDay)) {
      val prevDay = thisDay.clone().asInstanceOf[Calendar]
      prevDay.add(Calendar.DAY_OF_MONTH, -1)
      Some(prevDay)
    } else {
      None
    }
  }

  /**
    * Given the year, month, and day as parameter strings, return the previous day as strings or None, if the earliest day is crossed.
    */
  def prevDay(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Option[(String, String, String)] = {
    prevDay(parametersToDay(year, month, day)) match {
      case Some(previousDay) => Some(dayToStrings(previousDay))
      case None => None
    }
  }

  /**
    * Return all prev days including the passed Calendar as a sequence, bounded by the earliest day
    */
  def prevDaysFrom(thisDay: Calendar): Seq[Calendar] = {
    new Iterator[Calendar] {
      var current: Option[Calendar] = Some(thisDay)

      override def hasNext = current != None

      override def next = current match {
        case Some(day) => {
          current = prevDay(day)
          day
        }
        case None => null
      }
    }.toSeq
  }

  /**
    * Return all days in a Calendar range a sequence, bounded by the earliest day
    */
  def dayRange(fromThisDay: Calendar, toThisDay: Calendar): Seq[Calendar] = {
    new Iterator[Calendar] {
      var current: Option[Calendar] = Some(toThisDay)

      override def hasNext = current != None

      override def next = current match {
        case Some(day) => {
          current = if (current.get.after(fromThisDay)) prevDay(day)
          else
            None
          day
        }
        case None => null
      }
    }.toSeq
  }

  /**
    * Convert a sequence of Calendars into string date triples (year, month, day).
    */
  def dayParameterRange(range: Seq[Calendar]): Seq[(String, String, String)] =
    range.map {
      dayToStrings(_)
    }

  /**
    * Convert a sequence of Calendars into string month tuples (year, month).
    */
  def monthParameterRange(range: Seq[Calendar]): Seq[(String, String)] =
    range.map {
      dayToStrings(_)
    }.map { case (year, month, day) => (year, month) }.distinct

  def dayRangeAsParams(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Seq[(String, String, String)] =
    prevDaysFrom(parametersToDay(year, month, day)).map {
      dayToStrings(_)
    }

  /**
    * Return this date (given as string parameters) and all earlier dates bounded by the earliest day as a sequence of string date triples (year, month, day)
    */
  def thisAndPrevDays(year: Parameter[String], month: Parameter[String], day: Parameter[String]) = dayRangeAsParams(year, month, day)

  /**
    * Return the last of month passed (given as string parameters) and all earlier dates bounded by the earliest day as a sequence of string date triples (year, month, day)
    */
  def thisAndPrevDays(year: Parameter[String], month: Parameter[String]): Seq[(String, String, String)] = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3)
  }

  /**
    * Return this month (given as string parameters) and all earlier months bounded by the earliest day as a sequence of string month tuple (year, month)
    */
  def thisAndPrevMonths(year: Parameter[String], month: Parameter[String]): Seq[(String, String)] = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3).map { case (year, month, day) => (year, month) }.distinct
  }

  /**
    * Given a Calendar, return the previous month as a Calendar or None, if the earliest day is crossed.
    */
  def prevMonth(thisDay: Calendar): Option[Calendar] = {
    if (thisDay.after(earliestDay)) {
      val prevDay = thisDay.clone().asInstanceOf[Calendar]
      prevDay.add(Calendar.MONTH, -1)
      Some(prevDay)
    } else {
      None
    }
  }

  /**
    * Given a month (passed as string parameters) return the previous month as a string month tuple (year, month) or None if earliest day is crossed.
    */
  def prevMonth(year: Parameter[String], month: Parameter[String]): Option[(String, String)] = {
    prevMonth(parametersToDay(year, month, p("01"))) match {
      case Some(previousDay) => Some((dayToStrings(previousDay)._1, dayToStrings(previousDay)._2))
      case None => None
    }
  }

  /**
    * Return the sequence of all days from today until the earliest day as string date triples (year, month, day)
    */
  def allDays() = {
    val (todaysYear, todaysMonth, todaysDay) = today
    thisAndPrevDays(todaysYear, todaysMonth, todaysDay)
  }

  /**
    * Return the sequence of all months from today until the earliest day as string month tuples (year, month)
    */
  def allMonths() = {
    val (todaysYear, todaysMonth, _) = today
    thisAndPrevMonths(todaysYear, todaysMonth)
  }

  /**
    * Return the all days of the month passed as string parameters from today as string date triples (year, month, day)
    */
  def allDaysOfMonth(year: Parameter[String], month: Parameter[String]) = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val days = ListBuffer[(String, String, String)]()

    var currentDate = lastOfMonth
    var firstOfMonthReached = false

    while (!firstOfMonthReached) {
      firstOfMonthReached = currentDate.get(Calendar.DAY_OF_MONTH) == 1
      days += dayToStrings(currentDate)
      currentDate.add(Calendar.DAY_OF_MONTH, -1)
    }

    days.toList
  }
}

/**
  * Defines parameters for monthly partitioned views along with date arithmetic methods.
  */
trait MonthlyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]
  val monthId: Parameter[String] = p(s"${year.v.get}${month.v.get}")

  /**
    * Return the previous month as a string month tuple (year, month) or None if earliest day is crossed.
    */
  def prevMonth() = DateParameterizationUtils.prevMonth(year, month)

  /**
    * Return this and the previous months as string month tuples (year, month) until earliest day is crossed.
    */
  def thisAndPrevMonths() = DateParameterizationUtils.thisAndPrevMonths(year, month)

  /**
    * Return this and the previous day as string date triples (year, month, day) until earliest day is crossed.
    */
  def thisAndPrevDays() = DateParameterizationUtils.thisAndPrevDays(year, month)

  /**
    * Return this and the previous day as string date triples (year, month, day) until a given Calendar is crossed.
    */
  def thisAndPrevDaysUntil(thisDay: Calendar) =
    DateParameterizationUtils.prevDaysFrom(thisDay)

  /**
    * Return the sequence of all days from today until the earliest day as string date triples (year, month, day)
    */
  def allDays() = DateParameterizationUtils.allDays()

  /**
    * Return the sequence of all months from today until the earliest day as string month tuples (year, month)
    */
  def allMonths() = DateParameterizationUtils.allMonths()

  /**
    * Return the sequence of a given number of months from today until the earliest day as string month tuples (year, month)
    */
  def lastMonths(c: Int) = {
    val to = DateParameterizationUtils.parametersToDay(year, month, p("1"))
    val from = to
    from.add(Calendar.MONTH, c)
    DateParameterizationUtils.monthParameterRange(DateParameterizationUtils.dayRange(from, to))
  }

  /**
    * Return the all days of the month as string date triples (year, month, day)
    */
  def allDaysOfMonth() = DateParameterizationUtils.allDaysOfMonth(year, month)
}

/**
  * Defines parameters for daily partitioned views along with date arithmetic methods.
  */
trait DailyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]
  val day: Parameter[String]
  val dateId: Parameter[String] = p(s"${year.v.get}${month.v.get}${day.v.get}")

  /**
    * Return the previous day as Strings or None, if the earliest day is crossed.
    */
  def prevDay() = DateParameterizationUtils.prevDay(year, month, day)

  /**
    * Return the previous month as a string month tuple (year, month) or None if earliest day is crossed.
    */
  def prevMonth() = DateParameterizationUtils.prevMonth(year, month)

  /**
    * Return this date and all earlier dates bounded by the earliest day as a sequence of string date triples (year, month, day)
    */
  def thisAndPrevDays() = DateParameterizationUtils.thisAndPrevDays(year, month, day)

  /**
    * Return this and the previous months as string month tuples (year, month) until earliest day is crossed.
    */
  def thisAndPrevMonths() = DateParameterizationUtils.thisAndPrevMonths(year, month)

  /**
    * Return the sequence of all days from today until the earliest day as string date triples (year, month, day)
    */
  def allDays() = DateParameterizationUtils.allDays()

  /**
    * Return the sequence of all months from today until the earliest day as string month tuples (year, month)
    */
  def allMonths() = DateParameterizationUtils.allMonths()

  /**
    * Return the sequence of a given number of months from today until the earliest day as string month tuples (year, month)
    */
  def lastMonths(c: Int) = {
    val to = DateParameterizationUtils.parametersToDay(year, month, day)
    val from = to
    from.add(Calendar.MONTH, c)
    DateParameterizationUtils.dayParameterRange(DateParameterizationUtils.dayRange(from, to))
  }

  /**
    * Return the sequence of a given number of days from today until the earliest day as string data triples (year, month, day)
    */
  def lastDays(c: Int) = {
    val to = DateParameterizationUtils.parametersToDay(year, month, day)
    val from = to
    from.add(Calendar.DATE, c)
    DateParameterizationUtils.dayParameterRange(DateParameterizationUtils.dayRange(from, to))
  }
}