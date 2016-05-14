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

import java.net.URLDecoder

import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.TypedAny.typedAny
import org.schedoscope.dsl.{ TypedAny, View }
import org.schedoscope.dsl.views.DateParameterizationUtils.{ thisAndPrevDays, thisAndPrevMonths }

import scala.Array.canBuildFrom;

class NoAugmentation extends ViewUrlParser.ParsedViewAugmentor

object ViewUrlParser {

  case class ParsedView(env: String, viewClass: Class[View], parameters: List[TypedAny])

  trait ParsedViewAugmentor {
    def augment(pv: ParsedView): ParsedView = pv
  }

  val NullValue = "\\s*null\\s*\\(\\s*\\)\\s*".r
  val BooleanValue = "\\s*t\\s*\\(\\s*((true)|(false))\\s*\\)\\s*".r
  val IntValue = "\\s*i\\s*\\(\\s*(\\d+)\\s*\\)\\s*".r
  val LongValue = "\\s*l\\s*\\(\\s*(\\d+)\\s*\\)\\s*".r
  val ByteValue = "\\s*b\\s*\\(\\s*(\\d+)\\s*\\)\\s*".r
  val FloatValue = "\\s*f\\s*\\(\\s*([-+]?[0-9]*\\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\\s*\\)\\s*".r
  val DoubleValue = "\\s*d\\s*\\(\\s*([-+]?[0-9]*\\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\\s*\\)\\s*".r
  val MonthlyParameterizationValue = "\\s*ym\\s*\\(\\s*(\\d{4})(0[1-9]|10|11|12)\\s*\\)\\s*".r
  val DailyParameterizationValue = "\\s*ymd\\s*\\(\\s*(\\d{4})(0[1-9]|10|11|12)(0[1-9]|[12][0-9]|30|31)\\s*\\)\\s*".r
  val MonthlyRangeParameter = "\\s*rym\\s*\\(\\s*(\\d{4})(0[1-9]|10|11|12)\\s*[-]\\s*(\\d{4})(0[1-9]|10|11|12)\\s*\\)\\s*".r
  val DailyRangeParameter = "\\s*rymd\\s*\\(\\s*(\\d{4})(0[1-9]|10|11|12)(0[1-9]|[12][0-9]|30|31)\\s*[-]\\s*(\\d{4})(0[1-9]|10|11|12)(0[1-9]|[12][0-9]|30|31)\\s*\\)\\s*".r
  val Enumeration = "\\s*(e|et|ei|el|eb|ef|ed|eym|eymd|erym|erymd)\\s*\\((.*)\\)\\s*".r

  def unquote(s: String) = s
    .replaceAllLiterally("\\(", "(")
    .replaceAllLiterally("\\)", ")")
    .replaceAllLiterally("\\\\", "\\")
    .replaceAllLiterally("\\,", ",")
    .replaceAllLiterally("\\-", "-")

  def typeBasicParameter(parameter: String) = parameter match {
    case NullValue()                         => List(null)
    case BooleanValue(b, _, _)               => List(typedAny(p(b.toBoolean)))
    case IntValue(d)                         => List(typedAny(p(d.toInt)))
    case LongValue(d)                        => List(typedAny(p(d.toLong)))
    case ByteValue(d)                        => List(typedAny(p(d.toByte)))
    case FloatValue(f)                       => List(typedAny(p(f.toFloat)))
    case DoubleValue(f)                      => List(typedAny(p(f.toDouble)))
    case MonthlyParameterizationValue(y, m)  => List(typedAny(p(y)), typedAny(p(m)))
    case DailyParameterizationValue(y, m, d) => List(typedAny(p(y)), typedAny(p(m)), typedAny(p(d)))
    case aStringValue                        => List(typedAny(p(unquote(aStringValue))))
  }

  def typeMonthlyRangeParameter(earlierYear: String, earlierMonth: String, laterYear: String, laterMonth: String): List[List[TypedAny]] =
    if (s"${earlierYear}${earlierMonth}" > s"${laterYear}${laterMonth}")
      typeMonthlyRangeParameter(laterYear, laterMonth, earlierYear, earlierMonth)
    else
      thisAndPrevMonths(p(laterYear), p(laterMonth))
        .takeWhile { case (year, month) => s"${earlierYear}${earlierMonth}" <= s"${year}${month}" }
        .map { case (year, month) => List(typedAny(p(year)), typedAny(p(month))) }
        .toList

  def typeDailyRangeParameter(earlierYear: String, earlierMonth: String, earlierDay: String, laterYear: String, laterMonth: String, laterDay: String): List[List[TypedAny]] =
    if (s"${earlierYear}${earlierMonth}${earlierDay}" > s"${laterYear}${laterMonth}${laterDay}")
      typeDailyRangeParameter(laterYear, laterMonth, laterDay, earlierYear, earlierMonth, earlierDay)
    else
      thisAndPrevDays(p(laterYear), p(laterMonth), p(laterDay))
        .takeWhile { case (year, month, day) => s"${earlierYear}${earlierMonth}${earlierDay}" <= s"${year}${month}${day}" }
        .map { case (year, month, day) => List(typedAny(p(year)), typedAny(p(month)), typedAny(p(day))) }
        .toList

  def typeEnumerationParameter(enumerationType: String, enumerationValues: String): List[List[TypedAny]] = {
    val basicType = enumerationType.tail
    val values = enumerationValues.replaceAllLiterally("\\,", "§pleasedontquotemebaby§").split(",").map {
      _.replaceAllLiterally("§pleasedontquotemebaby§", "\\,")
    }.toList

    values.flatMap {
      value =>
        if (basicType.isEmpty)
          typeParameter(value)
        else
          typeParameter(s"${basicType}(${value})")
    }
  }

  def typeParameter(parameter: String) = parameter match {
    case MonthlyRangeParameter(earlierYear, earlierMonth, laterYear, laterMonth) => typeMonthlyRangeParameter(earlierYear, earlierMonth, laterYear, laterMonth)
    case DailyRangeParameter(earlierYear, earlierMonth, earlierDay, laterYear, laterMonth, laterDay) => typeDailyRangeParameter(earlierYear, earlierMonth, earlierDay, laterYear, laterMonth, laterDay)
    case Enumeration(enumerationType, enumerationValues) => typeEnumerationParameter(enumerationType, enumerationValues)
    case _ => List(typeBasicParameter(parameter))
  }

  def parseParameters(parameters: List[String], argumentLists: List[List[TypedAny]] = List(List())): List[List[TypedAny]] =
    if (parameters.isEmpty)
      argumentLists
    else {
      val parameter :: remainingParameters = parameters

      parseParameters(remainingParameters,
        for (existingArguments <- argumentLists; newArguments <- typeParameter(parameter))
          yield existingArguments ++ newArguments)
    }

  def parseViewClassnames(pakkage: String, viewClassNames: String) = try {
    viewClassNames match {
      case Enumeration(enumerationType, enumerationValues) => {
        if ("e".equals(enumerationType))
          enumerationValues.split(",").toSeq.map(vc => Class.forName(s"${pakkage}.${vc}").asInstanceOf[Class[View]]).toList
        else
          throw new IllegalArgumentException("Illegal view enumeration: Please use syntax 'e(view1,view2,...)")
      }
      case _ => List(Class.forName(s"${pakkage}.${viewClassNames}").asInstanceOf[Class[View]])
    }
  } catch {
    case cnf: ClassNotFoundException => throw new IllegalArgumentException("No class for package and view: " + cnf.getMessage())
  }

  def parse(env: String, viewUrlPath: String): List[ParsedView] = try {
    val normalizedPathFront = if (viewUrlPath.startsWith("/"))
      viewUrlPath.tail
    else
      viewUrlPath

    val normalizedPath = if (normalizedPathFront.endsWith("/"))
      normalizedPathFront.substring(0, normalizedPathFront.length() - 1)
    else
      normalizedPathFront

    val urlPathChunks = normalizedPath.split("/").map {
      URLDecoder.decode(_, "UTF-8")
    }.toList
    if (urlPathChunks.size < 2)
      throw new IllegalArgumentException("View URL paths needs at least a package and a view class name.")

    val packageName :: viewClassNames :: parameters = urlPathChunks

    for {
      viewClass <- parseViewClassnames(packageName, viewClassNames)
      pl <- parseParameters(parameters)
    } yield ParsedView(env, viewClass, pl)

  } catch {

    case e: Throwable => throw new IllegalArgumentException(
      s"""${viewUrlPath}

Problem: ${e.getMessage}

Little path format guide
========================

/{package}/{view}(/{view parameter value})*

View parameter value format:
  i(aNumber)                    => an integer
  l(aNumber)                    => a long
  b(aNumber)                    => a byte
  t(true)|t(false)              => a boolean
  f(aFloat)                     => a float
  d(aDouble)                    => a double
  ym(yyyyMM)                    => a MonthlyParameterization
  ymd(yyyyMMdd)                 => a DailyParameterization
  null()                        => null
  everything else               => a string

Ranges on view parameter values:
  rym(yyyyMM-yyyyMM)            => all MonthlyParameterizations between the first (earlier) and the latter (later)
  rymd(yyyyMMdd-yyyyMMdd)       => all DailyParameterizations between the first (earlier) and the latter (later)
  e{constructor parameter value format}({aValue},{anotherValue})
                                => enumerate multiple values for a given view parameter value format.
  For instance: 
    ei(1,2,3)                   => an enumeration of integer view parameters 
    e(aString, anotherString)   => an enumeration of string view parameters 
    eymd(yyyyMM,yyyMM)          => an enumeration of MonthlyParameterizations
    erymd(yyyyMM-yyyyMM,yyyyMM-yyyyMM) => an enumeration of MonthlyParameterization ranges

Quoting:
  Use backslashes to escape the syntax given above. The following characters need quotation: \\,(-)
""")
  }

  def viewNames(viewUrlPath: String) = parse("dev", viewUrlPath).map(pv => pv.viewClass.getName)
}