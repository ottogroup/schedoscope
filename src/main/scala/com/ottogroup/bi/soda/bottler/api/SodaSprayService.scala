package com.ottogroup.bi.soda.bottler.api

import spray.json._
import spray.routing.SimpleRoutingApp
import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport._
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.bottler.driver.DriverRunHandle
import com.ottogroup.bi.soda.bottler.SodaRootActor._


object SodaSprayService extends App with SimpleRoutingApp {

  implicit val system = settings.system

  import SodaJsonProtocol._
  
  startServer( interface = "localhost", port = 8888 ) {
                  
    get {
      path("foo") {
        complete {
          val v = View.viewsFromUrl("dev", "/app.eci.datahub/WebtrendsEvent/EC0106/2014/01/01/20140101")
          //val ar = Act
          Edge(1, 1)
          v
        }
    }
  }
  
}