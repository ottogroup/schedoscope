package com.ottogroup.bi.soda.dsl.views

import java.util.Calendar
import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.Parameter.p
import com.ottogroup.bi.soda.bottler.api.Settings

object DateParameterizationUtils {
  def earliestDay = Settings().earliestDay

  def parametersToDay(year: Parameter[String], month: Parameter[String], day: Parameter[String]) = {
    val date = Calendar.getInstance()
    date.clear()
    date.set(year.v.get.toInt, month.v.get.toInt - 1, day.v.get.toInt)

    date
  }

  def dayToParameters(thisDay: Calendar) = {
    val year: Parameter[String] = p(s"${"%04d".format(thisDay.get(Calendar.YEAR))}")
    val month: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.MONTH) + 1)}")
    val day: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.DAY_OF_MONTH))}")

    (year, month, day)
  }

  def today = dayToParameters(Settings().latestDay)

  def prevDay(thisDay: Calendar): Option[Calendar] = {
    if (thisDay.after(earliestDay)) {
      val prevDay = thisDay.clone().asInstanceOf[Calendar]
      prevDay.add(Calendar.DAY_OF_MONTH, -1)
      Some(prevDay)
    } else {
      None
    }
  }

  def prevDay(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Option[(Parameter[String], Parameter[String], Parameter[String])] = {
    prevDay(parametersToDay(year, month, day)) match {
      case Some(previousDay) => Some(dayToParameters(previousDay))
      case None => None
    }
  }

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

  def thisAndPrevDays(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Seq[(Parameter[String], Parameter[String], Parameter[String])] =
    prevDaysFrom(parametersToDay(year, month, day)).map { dayToParameters(_) }

  def thisAndPrevDays(year: Parameter[String], month: Parameter[String]): Seq[(Parameter[String], Parameter[String], Parameter[String])] = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3)
  }

  def thisAndPrevMonths(year: Parameter[String], month: Parameter[String]): Seq[(Parameter[String], Parameter[String])] = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3).map { case (year, month, day) => (year, month) }.distinct
  }

  def allDays() = {
    val (todaysYear, todaysMonth, todaysDay) = today
    thisAndPrevDays(todaysYear, todaysMonth, todaysDay)
  }

  def allMonths() = {
    val (todaysYear, todaysMonth, _) = today
    thisAndPrevMonths(todaysYear, todaysMonth)
  }
}

trait MonthlyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]

  def thisAndPrevMonths() = DateParameterizationUtils.thisAndPrevMonths(year, month)

  def thisAndPrevDays() = DateParameterizationUtils.thisAndPrevDays(year, month)

  def allDays() = DateParameterizationUtils.allDays()

  def allMonths() = DateParameterizationUtils.allMonths()
}

trait DailyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]
  val day: Parameter[String]

  val dateId: Parameter[String] = p(s"${year.v.get}${month.v.get}${day.v.get}")

  def prevDay() = DateParameterizationUtils.prevDay(year, month, day)

  def thisAndPrevDays() = DateParameterizationUtils.thisAndPrevDays(year, month, day)

  def allDays() = DateParameterizationUtils.allDays()
}