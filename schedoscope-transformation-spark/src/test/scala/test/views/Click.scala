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
import org.schedoscope.dsl.transformations.{HiveTransformation, SparkSQLRunner, SparkTransformation}
import org.schedoscope.dsl.transformations.SparkTransformation._
import org.schedoscope.dsl.transformations.HiveTransformation._
import org.schedoscope.dsl.views.{DailyParameterization, Id}
import org.schedoscope.dsl.{Parameter, View}


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
      "ClickOfEC0101ViaSpark",
      jarOf(SparkSQLRunner), classNameOf(SparkSQLRunner),
      List(
        s"""
           INSERT INTO TABLE ${this.tableName}
           PARTITION (year = '${year.v.get}', month = '${month.v.get}', day = '${day.v.get}', date_id = '${dateId.v.get}')
           SELECT *
           FROM ${click().tableName}
           WHERE shop_code = 'EC0101'
           AND   year = '${year.v.get}' AND month = '${month.v.get}' AND day = '${day.v.get}' AND date_id = '${dateId.v.get}'
         """.stripMargin
      )
    )
  )
}

case class ClickOfEC0101ViaHiveQlOnSpark(year: Parameter[String],
                                         month: Parameter[String],
                                         day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(() => runOnSpark(
    HiveTransformation(
      insertInto(this,
        s"""
           SELECT *
           FROM ${click().tableName}
           WHERE shop_code = 'EC0101'
           AND   year = '${year.v.get}' AND month = '${month.v.get}' AND day = '${day.v.get}' AND date_id = '${dateId.v.get}'
         """)
    ))
  )
}
