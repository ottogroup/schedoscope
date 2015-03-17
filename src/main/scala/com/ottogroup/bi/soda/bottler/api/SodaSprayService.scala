package com.ottogroup.bi.soda.bottler.api

import spray.json._
import spray.routing.SimpleRoutingApp
import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport._
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.ActionStatusResponse
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.dsl.Transformation

object SodaSprayService extends App with SimpleRoutingApp {

  implicit val system = ActorSystem("soda-webservice")
  val soda = new SodaSystem()
  import SodaJsonProtocol._

  startServer(interface = "localhost", port = 8888) {    
    path("actions") {
      get {
        complete {
          soda.actions("")
        }
      }
    } ~    
    path("views") {
      get {
        complete {
          soda.views("", "")
        }
      }
    } ~
    path("materialize" / Rest) { viewUrlPath =>
      {
        get {
          complete {
            soda.materialize(viewUrlPath)
          }
        }
      }
    } ~
    path("command" / Rest) { commandId =>
      {
        get {
          complete {
            soda.commandStatus(commandId)
          }
        }
      }
    }    
  }

}

//          val v = View.viewsFromUrl("dev", "/app.eci.datahub/WebtrendsEvent/EC0106/2014/01/01/20140101")
//          val h = new DriverRunHandle[HiveTransformation](null, new LocalDateTime(), HiveTransformation("THIS IS SQL"), null)
//          var ar = new ActionStatusResponse[HiveTransformation]("message!", null, null, h, null)
//          Edge(1, 1)
