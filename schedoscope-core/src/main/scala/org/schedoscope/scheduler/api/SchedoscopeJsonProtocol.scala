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
package org.schedoscope.scheduler.api

import scala.concurrent.Future
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunOngoing
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import spray.json.DefaultJsonProtocol
import spray.json.JsonFormat

case class SchedoscopeCommand(id: String, start: String, parts: List[Future[_]])
case class SchedoscopeCommandStatus(id: String, start: String, end: Option[String], status: Map[String, Int])
case class TransformationStatus(actor: String, typ: String, status: String, runStatus: Option[RunStatus], properties: Option[Map[String, String]])
case class TransformationStatusList(overview: Map[String, Int], actions: List[TransformationStatus])
case class ViewStatus(view: String, status: String, properties: Option[Map[String, String]], dependencies: Option[List[ViewStatus]])
case class ViewStatusList(overview: Map[String, Int], views: List[ViewStatus])
case class QueueStatusList(overview: Map[String, Int], queues: Map[String, List[RunStatus]])
case class RunStatus(description: String, targetView: String, started: String, comment: String, properties: Option[Map[String, String]])

object SchedoscopeJsonProtocol extends DefaultJsonProtocol {

  val formatter = DateTimeFormat.shortDateTime()

  implicit val runStatusFormat = jsonFormat5(RunStatus)
  implicit val actionStatusFormat = jsonFormat5(TransformationStatus)
  implicit val actionStatusListFormat = jsonFormat2(TransformationStatusList)
  implicit val schedoscopeCommandStatusFormat = jsonFormat4(SchedoscopeCommandStatus)
  implicit val viewStatusFormat: JsonFormat[ViewStatus] = lazyFormat(jsonFormat4(ViewStatus))
  implicit val viewStatusListFormat = jsonFormat2(ViewStatusList)
  implicit val queueStatusListFormat = jsonFormat2(QueueStatusList)

  /*
   FIXME: deactivated LocalDateTime (de)serialization due to strange NPEs,
          changed LocalDateTime fields (start, end, ...) to String
  implicit val localDateTimeFormat: JsonFormat[LocalDateTime] = LocalDateTimeSerDe
  implicit object LocalDateTimeSerDe extends RootJsonFormat[LocalDateTime] {

    def read(value: JsValue) = try {
      value match {
        case s: JsString => formatter.parseDateTime(s.value).toLocalDateTime()
        case _ => new LocalDateTime(0)
      }
    } catch { case t: Throwable => new LocalDateTime(0) }

    def write(d: LocalDateTime) = try {
        JsString(formatter.print(d))
      }
      catch { case t : Throwable => JsString("")}
  }
  */

  def formatDate(d: LocalDateTime): String = {
    if (d != null) formatter.print(d) else ""
  }

  def parseActionStatus(a: TransformationStatusResponse[_]): TransformationStatus = {
    val actor = getOrElse(a.actor.path.toStringWithoutAddress, "unknown")
    val typ = if (a.driver != null) getOrElse(a.driver.transformationName, "unknown") else "unknown"
    var drh = a.driverRunHandle
    var status = if (a.message != null) a.message else "none"
    var comment = ""

    if (a.driverRunStatus != null) {
      a.driverRunStatus.asInstanceOf[DriverRunState[Any with Transformation]] match {
        case s: DriverRunSucceeded[_] => { comment = getOrElse(s.comment, "no-comment"); status = "succeeded" }
        case f: DriverRunFailed[_]    => { comment = getOrElse(f.reason, "no-reason"); status = "failed" }
        case o: DriverRunOngoing[_]   => { drh = o.runHandle }
      }
    }

    if (drh != null) {
      val desc = drh.transformation.asInstanceOf[Transformation].description
      val view = drh.transformation.asInstanceOf[Transformation].getView()
      val started = drh.started
      val runStatus = RunStatus(getOrElse(desc, "no-desc"), getOrElse(view, "no-view"), getOrElse(formatDate(started), ""), comment, None)
      TransformationStatus(actor, typ, status, Some(runStatus), None)
    } else {
      TransformationStatus(actor, typ, status, None, None)
    }
  }

  def parseQueueElements(q: List[AnyRef]): List[RunStatus] = {
    q.map(o =>
      if (o.isInstanceOf[Transformation]) {
        val trans = o.asInstanceOf[Transformation]
        RunStatus(trans.description, trans.getView(), "", "", None)
      } else {
        RunStatus(o.toString, "", "", "", None)
      })
  }

  def getOrElse[T](o: T, d: T) = {
    if (o != null) o else d;
  }

}
