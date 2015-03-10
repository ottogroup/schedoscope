package com.ottogroup.bi.soda.bottler.api

import spray.json._
import com.ottogroup.bi.soda.bottler.ViewStatus

// FIXME: we should unify this, staying as close as possible to existing objects...

case class Proc(status: String, typ: String, start: String, job: String)
case class ProcList(running: Int, idle: Int, processes: List[Proc])
case class ViewStat(status: String, name: String)
case class ViewSpec(status: String, view: String, parameters: String)
case class ViewList(overview: Map[String, String], details: List[ViewSpec])

object SodaJsonProtocol extends DefaultJsonProtocol {
  implicit val processFormat = jsonFormat4(Proc)
  implicit val procListFormat = jsonFormat3(ProcList)
  implicit val viewStatFormat = jsonFormat2(ViewStat)
  implicit val viewSpecFormat = jsonFormat3(ViewSpec)
  implicit val viewListFormat = jsonFormat2(ViewList)
}