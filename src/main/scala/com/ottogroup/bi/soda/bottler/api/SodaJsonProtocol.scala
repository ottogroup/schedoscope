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

// FIXME: we should unify this, staying as close as possible to existing objects...

case class Proc(status: String, typ: String, start: String, transformation: String)
case class ProcList(running: Int, idle: Int, queued: Int, queues: Map[String, List[String]], processes: List[Proc])
case class ViewStat(status: String, view: String)
case class ViewSpec(status: String, view: String, parameters: String)
case class ViewList(overview: Map[String, Int], details: List[ViewSpec])

case class SodaCommandStatus(id: String, started: LocalDateTime, progress: Double, status: String)
case class ActionStatusList(overview: Map[String,Int], details: List[ActionStatusResponse[_]])
case class ViewStatusList(overview: Map[String,Int], details: List[ViewStatusResponse])
case class Edge(source: Int, target: Int)
case class DependencyGraph(views: List[View], dependencies: List[Edge])


object SodaJsonProtocol extends DefaultJsonProtocol {
  implicit val processFormat = jsonFormat4(Proc)
  implicit val procListFormat = jsonFormat5(ProcList)
  implicit val viewStatFormat = jsonFormat2(ViewStat)
  implicit val viewSpecFormat = jsonFormat3(ViewSpec)
  implicit val viewListFormat = jsonFormat2(ViewList)
  implicit val edgeFormat = jsonFormat2(Edge)
  
  implicit object viewFormat extends RootJsonFormat[View] {
    def read(value : JsValue) = {
      null
    }
    def write (v: View) = {
      JsObject(Map("urlPath" -> JsString(v.viewId)))
    }
  } 
  
//  implicit object actionFormat extends RootJsonFormat[_] {
//    
//  }
  
}
