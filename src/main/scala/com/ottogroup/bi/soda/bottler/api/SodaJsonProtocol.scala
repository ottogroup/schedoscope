package com.ottogroup.bi.soda.bottler.api

import spray.json.DefaultJsonProtocol
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.ActionStatusResponse
import com.ottogroup.bi.soda.bottler.ViewStatusResponse
import com.ottogroup.bi.soda.dsl.View
import spray.json.RootJsonFormat
import spray.json.JsValue
import spray.json.JsString
import spray.json.JsObject
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.ActionStatusResponse
import spray.json.JsArray
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import org.joda.time.format.DateTimeFormat
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation
import scala.concurrent.Future
import spray.json.JsonFormat
import com.ottogroup.bi.soda.bottler.driver.DriverRunState

// FIXME: we should unify this, staying as close as possible to existing objects...

case class Proc(status: String, typ: String, start: String, transformation: String)
case class ProcList(running: Int, idle: Int, queued: Int, queues: Map[String, List[String]], processes: List[Proc])
case class ViewStat(status: String, view: String)
case class ViewSpec(status: String, view: String, parameters: String)
case class ViewList(overview: Map[String, Int], details: List[ViewSpec])

case class SodaCommand(id: String, started: LocalDateTime, parts: List[Future[_]])
case class SodaCommandStatus(id: String, started: LocalDateTime, status: Map[String,Int])
case class ActionStatusList(overview: Map[String, Int], queues: Map[String,List[String]], actions: List[ActionStatusResponse[_]])
case class ViewStatusList(overview: Map[String, Int], views: List[ViewStatusResponse])
case class Edge(source: Int, target: Int)
case class DependencyGraph(nodes: List[View], dependencies: List[Edge])


object SodaJsonProtocol extends DefaultJsonProtocol {

  implicit val processFormat = jsonFormat4(Proc)
  implicit val procListFormat = jsonFormat5(ProcList)
  implicit val viewStatFormat = jsonFormat2(ViewStat)
  implicit val viewSpecFormat = jsonFormat3(ViewSpec)
  implicit val viewListFormat = jsonFormat2(ViewList)
  implicit val actionStatusListFormat = jsonFormat3(ActionStatusList)
  implicit val edgeFormat = jsonFormat2(Edge)
  implicit val depGraphFormat = jsonFormat2(DependencyGraph)
  implicit val viewFormat: JsonFormat[View] = viewSerDe
  implicit val actionStatusResponseFormat: JsonFormat[ActionStatusResponse[_]] = actionStatusResponseSerDe
  implicit val sodaCommandStatusFormat = jsonFormat3(SodaCommandStatus)
  implicit val viewStatusResponseFormat = jsonFormat2(ViewStatusResponse)
  implicit val viewStatusListFormat = jsonFormat2(ViewStatusList)
  implicit val localDateTimeFormat : JsonFormat[LocalDateTime] = localDateTimeSerDe

  implicit object localDateTimeSerDe extends RootJsonFormat[LocalDateTime] {
    val formatter = DateTimeFormat.shortDateTime()
    def read(value: JsValue) = {
      null // FIXME: do we ever want to parse dates?
    }
    def write(d: LocalDateTime) = {
      JsString(formatter.print(d))
    } 
  }
  
  implicit object viewSerDe extends RootJsonFormat[View] {
    def read(value: JsValue) = {
      null // FIXME: do we ever want to parse a view from JSON?
    }
    def write(v: View) = {
      JsObject(Map("urlPath" -> JsString(v.viewId)))
    }
  }

//  implicit object hiveTransformationFormat extends RootJsonFormat[ActionStatusResponse[HiveTransformation]] with transformationFormat[HiveTransformation]
//  implicit object oozieTransformationFormat extends RootJsonFormat[ActionStatusResponse[OozieTransformation]] with transformationFormat[OozieTransformation]
//  // FIXME: all those bloody subclasses from FileSysTransformation
//  implicit object filesystemTransformationFormat extends RootJsonFormat[ActionStatusResponse[FilesystemTransformation]] with transformationFormat[FilesystemTransformation]

  
  implicit object actionStatusResponseSerDe extends RootJsonFormat[ActionStatusResponse[_]] {
   def read(value: JsValue) = {
      null
    }
    def write(a: ActionStatusResponse[_]) = {
      var status = a.message
      val actor = a.actor.path.toStringWithoutAddress
      var drh: DriverRunHandle[_] = null
      var comment: String = ""
      if (a.driverRunHandle != null) {
        drh = a.driverRunHandle
      }
      if (a.driverRunStatus != null) {
        val drs = a.driverRunStatus.asInstanceOf[DriverRunState[Any with Transformation]] // FIXME: does this work?
        drs match {
          case s: DriverRunSucceeded[_] => comment = s.comment
          case o: DriverRunOngoing[_]   => drh = o.runHandle
          case f: DriverRunFailed[_]    => comment = f.reason
        }
      }
      if (drh != null)
        JsObject(Map("status" -> JsString(status),
          "actor" -> JsString(actor),
          "type" -> JsString(drh.transformation.asInstanceOf[Transformation].name),
          "targetView" -> JsString(drh.transformation.asInstanceOf[Transformation].getView()),
          "started" -> localDateTimeSerDe.write(drh.started),
          "comment" -> JsString(comment)))
      else
        JsObject(Map("status" -> JsString(status),
          "actor" -> JsString(actor)))
    }    
  }
  
//  trait transformationFormat[T <: Transformation] {
//    def read(value: JsValue) = {
//      null
//    }
//    def write(a: ActionStatusResponse[T]) = {
//      var status = a.message
//      val actor = "ACTOR" // FIXME: a.actor.path.toStringWithoutAddress
//      var drh: DriverRunHandle[T] = null
//      var comment: String = ""
//      if (a.driverRunHandle != null) {
//        drh = a.driverRunHandle
//      }
//      if (a.driverRunStatus != null) {
//        val drs = a.driverRunStatus
//        drs match {
//          case s: DriverRunSucceeded[T] => comment = s.comment
//          case o: DriverRunOngoing[T]   => drh = o.runHandle
//          case f: DriverRunFailed[T]    => comment = f.reason
//        }
//      }
//      if (drh != null)
//        JsObject(Map("status" -> JsString(status),
//          "actor" -> JsString(actor),
//          "type" -> JsString(drh.transformation.asInstanceOf[Transformation].name),
//          "targetView" -> JsString(drh.transformation.asInstanceOf[Transformation].getView()),
//          "started" -> JsString(formatter.print(drh.started)),
//          "comment" -> JsString(comment)))
//      else
//        JsObject(Map("status" -> JsString(status),
//          "actor" -> JsString(actor)))
//    }
//  }

}
