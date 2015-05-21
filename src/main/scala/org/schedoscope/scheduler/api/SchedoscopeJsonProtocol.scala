package org.schedoscope.scheduler.api

import scala.concurrent.Future
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import org.schedoscope.scheduler.ActionStatusResponse
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunOngoing
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import org.schedoscope.dsl.Transformation
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonFormat
import spray.json.RootJsonFormat
import spray.json.NullOptions

case class SchedoscopeCommand(id: String, start: LocalDateTime, parts: List[Future[_]])
case class SchedoscopeCommandStatus(id: String, start: LocalDateTime, end: Option[LocalDateTime], status: Map[String, Int])
case class ActionStatus(actor: String, typ: String, status: String, runStatus: Option[RunStatus], properties: Option[Map[String, String]])
case class ActionStatusList(overview: Map[String, Int], actions: List[ActionStatus])
case class ViewStatus(view: String, status: String, properties: Option[Map[String, String]], dependencies: Option[List[ViewStatus]])
case class ViewStatusList(overview: Map[String, Int], views: List[ViewStatus])
case class QueueStatusList(overview: Map[String, Int], queues: Map[String, List[RunStatus]])
case class RunStatus(description: String, targetView: String, started: LocalDateTime, comment: String, properties: Option[Map[String, String]])

object SchedoscopeJsonProtocol extends DefaultJsonProtocol {

  implicit val runStatusFormat = jsonFormat5(RunStatus)
  implicit val actionStatusFormat : JsonFormat[ActionStatus] = lazyFormat(jsonFormat5(ActionStatus))
  implicit val actionStatusListFormat = jsonFormat2(ActionStatusList)
  implicit val schedoscopeCommandStatusFormat = jsonFormat4(SchedoscopeCommandStatus)
  implicit val viewStatusFormat: JsonFormat[ViewStatus] = lazyFormat(jsonFormat4(ViewStatus))
  implicit val viewStatusListFormat = jsonFormat2(ViewStatusList)
  implicit val queueStatusListFormat = jsonFormat2(QueueStatusList)
  implicit val localDateTimeFormat: JsonFormat[LocalDateTime] = localDateTimeSerDe

  implicit object localDateTimeSerDe extends RootJsonFormat[LocalDateTime] {
    val formatter = DateTimeFormat.shortDateTime()

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

  def parseActionStatus(a: ActionStatusResponse[_]): ActionStatus = {
    val actor = getOrElse(a.actor.path.toStringWithoutAddress, "unknown")
    val typ = if (a.driver != null) getOrElse(a.driver.transformationName, "unknown") else "unknown"
    var drh = a.driverRunHandle
    var status = if (a.message != null) a.message else "none"
    var comment = ""

    if (a.driverRunStatus != null) {
      a.driverRunStatus.asInstanceOf[DriverRunState[Any with Transformation]] match {
        case s: DriverRunSucceeded[_] => { comment = getOrElse(s.comment,"no-comment"); status = "succeeded" }
        case f: DriverRunFailed[_]    => { comment = getOrElse(f.reason, "no-reason"); status = "failed" }
        case o: DriverRunOngoing[_]   => { drh = o.runHandle }
      }
    }

    if (drh != null) {
      val desc = drh.transformation.asInstanceOf[Transformation].description
      val view = drh.transformation.asInstanceOf[Transformation].getView()
      val started = drh.started
      val runStatus = RunStatus(getOrElse(desc,"no-desc"), getOrElse(view,"no-view"), getOrElse(started,new LocalDateTime(0)), comment, None)
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
  
  def getOrElse[T](o: T, d: T) = {
    if (o != null) o else d;
  }

}
