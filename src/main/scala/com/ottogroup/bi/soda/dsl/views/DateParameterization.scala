package com.ottogroup.bi.soda.dsl.views

import java.util.Calendar

import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter.p

object DateParameterizationUtils {
  def defaultEarliestDay = {
    val cal = Calendar.getInstance()
    cal.clear()
    cal.set(2013, Calendar.DECEMBER, 1)
    cal
  }

  def defaultLatestDay = {
    val cal = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal
  }

  def dayToParameters(thisDay: Calendar) = {
    val year: Parameter[String] = p(s"${"%04d".format(thisDay.get(Calendar.YEAR))}")
    val month: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.MONTH) + 1)}")
    val day: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.DAY_OF_MONTH))}")

    (year, month, day)
  }

  def parametersToDate(year: Parameter[String], month: Parameter[String], day: Parameter[String]) = {
    val date = Calendar.getInstance()
    date.clear()
    date.set(year.v.get.toInt, month.v.get.toInt - 1, day.v.get.toInt)

    date
  }

  def today = dayToParameters(defaultLatestDay)

  def prevDay(thisDay: Calendar, earliestDay: Calendar): Option[Calendar] = {
    if (thisDay.after(earliestDay)) {
      val prevDay = thisDay.clone().asInstanceOf[Calendar]
      prevDay.add(Calendar.DAY_OF_MONTH, -1)
      Some(prevDay)
    } else {
      None
    }
  }

  def prevDay(year: Parameter[String], month: Parameter[String], day: Parameter[String], earliestDay: Calendar = defaultEarliestDay): Option[(Parameter[String], Parameter[String], Parameter[String])] = {
    prevDay(parametersToDate(year, month, day), earliestDay) match {
      case Some(previousDay) => Some(dayToParameters(previousDay))
      case None => None
    }
  }

  def prevDaysFrom(thisDay: Calendar, earliestDay: Calendar): Seq[Calendar] = {
    new Iterator[Calendar] {
      var current: Option[Calendar] = Some(thisDay)

      override def hasNext = current != None

      override def next = current match {
        case Some(day) => {
          current = prevDay(day, earliestDay)
          day
        }
        case None => null
      }
    }.toSeq
  }

  def thisAndPrevDays(year: Parameter[String], month: Parameter[String], day: Parameter[String], earliestDay: Calendar): Seq[(Parameter[String], Parameter[String], Parameter[String])] =
    prevDaysFrom(parametersToDate(year, month, day), earliestDay).map { dayToParameters(_) }

  def thisAndPrevDays(year: Parameter[String], month: Parameter[String], earliestDay: Calendar): Seq[(Parameter[String], Parameter[String], Parameter[String])] = {
    val lastOfMonth = parametersToDate(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3, earliestDay)
  }

  def thisAndPrevMonths(year: Parameter[String], month: Parameter[String], earliestDay: Calendar): Seq[(Parameter[String], Parameter[String])] = {
    val lastOfMonth = parametersToDate(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3, earliestDay).map { case (year, month, day) => (year, month) }.distinct
  }

  def allDays(earliestDay: Calendar = defaultEarliestDay) = {
    val (todaysYear, todaysMonth, todaysDay) = today
    thisAndPrevDays(todaysYear, todaysMonth, todaysDay, earliestDay)
  }

  def allMonths(earliestDay: Calendar = defaultEarliestDay) = {
    val (todaysYear, todaysMonth, _) = today
    thisAndPrevMonths(todaysYear, todaysMonth, earliestDay)
  }
}

trait MonthlyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]

  def thisAndPrevMonths(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.thisAndPrevMonths(year, month, earliestDay)

  def thisAndPrevDays(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.thisAndPrevDays(year, month, earliestDay)

  def allDays(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.allDays(earliestDay)

  def allMonths(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.allMonths(earliestDay)

}

trait DailyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]
  val day: Parameter[String]

  val dateId: Parameter[String] = p(s"${year.v.get}${month.v.get}${day.v.get}")

  def prevDay(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.prevDay(year, month, day, earliestDay)

  def thisAndPrevDays(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.thisAndPrevDays(year, month, day, earliestDay)

  def allDays(earliestDay: Calendar = DateParameterizationUtils.defaultEarliestDay) = DateParameterizationUtils.allDays(earliestDay)
}