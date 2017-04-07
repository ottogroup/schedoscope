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

import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.{Parameter, View}
import org.schedoscope.dsl.transformations.OozieTransformation
import org.schedoscope.dsl.transformations.OozieTransformation.oozieWFPath
import org.schedoscope.dsl.views.{DailyParameterization, Id}


case class Click(shopCode: Parameter[String],
                 year: Parameter[String],
                 month: Parameter[String],
                 day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]
}

case class ClickOfEC0101ViaOozie(year: Parameter[String],
                                 month: Parameter[String],
                                 day: Parameter[String]) extends View
  with Id
  with DailyParameterization {

  val url = fieldOf[String]

  val click = dependsOn(() => Click(p("EC0101"), year, month, day))

  transformVia(
    () => OozieTransformation(
      "bundle", "click",
      oozieWFPath("bundle", "click")))
}
