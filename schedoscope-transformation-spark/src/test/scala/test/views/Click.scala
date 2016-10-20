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
package test.views

import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.SparkTransformation
import org.schedoscope.dsl.transformations.SparkTransformation._
import org.schedoscope.dsl.views.{DailyParameterization, Id}
import org.schedoscope.dsl.{Parameter, View}
import org.schedoscope.spark.test.ClickOfEC0101Transformation


case class Click(shopCode: Parameter[String],
                 year: Parameter[String],
                 month: Parameter[String],
                 day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]
}


case class ClickOfEC0101ViaSpark(year: Parameter[String],
                                 month: Parameter[String],
                                 day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(() =>
    SparkTransformation(
      classNameOf(ClickOfEC0101Transformation), jarOf(ClickOfEC0101Transformation), classNameOf(ClickOfEC0101Transformation),
      List(click().tableName, tableName, "EC0101", year.v.get, month.v.get, day.v.get, dateId.v.get)
    ))

}
