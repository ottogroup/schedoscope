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
package schedoscope.example.osm

import java.text.SimpleDateFormat
import java.util.Date

import org.schedoscope.Settings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.views.MonthlyParameterization

object Globals {
  def defaultHiveQlParameters(v: View) = {
    val baseParameters = Map(
      "env" -> v.env,
      "workflow_time" -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date),
      "workflow_name" -> v.getClass().getName())

    if (v.isInstanceOf[MonthlyParameterization])
      baseParameters ++ Map(
        "year" -> v.asInstanceOf[MonthlyParameterization].year.v.get,
        "month" -> v.asInstanceOf[MonthlyParameterization].month.v.get)
    else baseParameters
  }

  def defaultPigProperties(v: View) = Map(
    "exec.type" -> "MAPREDUCE",
    "mapred.job.tracker" -> Settings().jobTrackerOrResourceManager,
    "fs.default.name" -> Settings().nameNode,
    "workflow_time" -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date),
    "workflow_name" -> v.getClass().getName())
}