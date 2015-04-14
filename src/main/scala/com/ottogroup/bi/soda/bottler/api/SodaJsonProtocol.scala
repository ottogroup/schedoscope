package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.Future

import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat

import com.ottogroup.bi.soda.bottler.ActionStatusResponse
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
import com.ottogroup.bi.soda.bottler.driver.DriverRunState
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.dsl.Transformation

import spray.json.DefaultJsonProtocol
import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonFormat
import spray.json.RootJsonFormat

case class SodaCommand(id: String, start: LocalDateTime, parts: List[Future[_]])
case class SodaCommandStatus(id: String, start: LocalDateTime, end: LocalDateTime, status: Map[String, Int])
case class ActionStatus(actor: String, typ: String, status: String, runStatus: Option[RunStatus], properties: Option[Map[String, String]])
case class ActionStatusList(overview: Map[String, Int], queues: Map[String, List[RunStatus]], actions: List[ActionStatus])
case class ViewStatus(view: String, status: String, properties: Option[Map[String, String]], dependencies: Option[List[ViewStatus]])
case class ViewStatusList(overview: Map[String, Int], views: List[ViewStatus])
case class RunStatus(description: String, targetView: String, started: LocalDateTime, comment: String, properties: Option[Map[String, String]])


object SodaJsonProtocol extends DefaultJsonProtocol {

  implicit val runStatusFormat = jsonFormat5(RunStatus)
  implicit val actionStatusFormat = jsonFormat5(ActionStatus)
  implicit val actionStatusListFormat = jsonFormat3(ActionStatusList)
  implicit val sodaCommandStatusFormat = jsonFormat4(SodaCommandStatus)
  implicit val viewStatusFormat: JsonFormat[ViewStatus] = lazyFormat(jsonFormat4(ViewStatus))
  implicit val viewStatusListFormat = jsonFormat2(ViewStatusList)
  implicit val localDateTimeFormat: JsonFormat[LocalDateTime] = localDateTimeSerDe

  implicit object localDateTimeSerDe extends RootJsonFormat[LocalDateTime] {
    val formatter = DateTimeFormat.shortDateTime()

    def read(value: JsValue) = {
      value match {
        case s: JsString => {
          try { formatter.parseDateTime(s.value).toLocalDateTime() }
          catch { case ie: IllegalArgumentException => null }
        }
        case _ => null
      }
    }

    def write(d: LocalDateTime) = {
      if (d == null)
        JsString("")
      else
        JsString(formatter.print(d))
    }
  }

  def parseActionStatus(a: ActionStatusResponse[_]): ActionStatus = {
    val actor = a.actor.path.toStringWithoutAddress
    val typ = if (a.driver != null) a.driver.transformationName else "unknown"
    var drh = a.driverRunHandle
    var status = a.message
    var comment: String = ""

    if (a.driverRunStatus != null) {
      a.driverRunStatus.asInstanceOf[DriverRunState[Any with Transformation]] match {
        case s: DriverRunSucceeded[_] => { comment = s.comment; status = "succeeded" }
        case f: DriverRunFailed[_] => { comment = f.reason; status = "failed" }
        case o: DriverRunOngoing[_] => { drh = o.runHandle }
      }
    }

    if (drh != null) {
      val desc = drh.transformation.asInstanceOf[Transformation].description
      val view = drh.transformation.asInstanceOf[Transformation].getView()
      val runStatus = RunStatus(desc, view, drh.started, comment, None)
      ActionStatus(actor, typ, status, Some(runStatus), None)
    } else {
      ActionStatus(actor, typ, status, None, None)
    }
  }

  def parseQueueElements(q: List[AnyRef]): List[RunStatus] = {
    q.map(o =>
      if (o.isInstanceOf[Transformation]) {
        val trans = o.asInstanceOf[Transformation]
        RunStatus(trans.description, trans.getView(), null, "", None)
      } else {
        RunStatus(o.toString, "", null, "", None)
      })
  }

}
