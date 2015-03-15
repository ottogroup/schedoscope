package com.ottogroup.bi.soda.bottler.api

import spray.json._
import spray.routing.SimpleRoutingApp
import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport._


object SodaSprayService extends App with SimpleRoutingApp {

  implicit val system = Settings().system
    
  import SodaJsonProtocol._
  
  startServer( interface = "localhost", port = 8888 ) {
                  
    get {
        path("foo") {
           complete {           
             Edge(1,1)
           }      
        }
    }
  }
  
}