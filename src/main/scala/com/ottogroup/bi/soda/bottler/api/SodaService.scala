package com.ottogroup.bi.soda.bottler.api

import com.ottogroup.bi.soda.bottler.SodaRootActor.settings

import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp

object SodaService extends App with SimpleRoutingApp {

  implicit val system = ActorSystem("soda-webservice")
  val soda = new SodaSystem()

  import SodaJsonProtocol._

  startServer(interface = "localhost", port = settings.port) {
    get {
      path("actions") {
        complete {
          soda.actions(None)
        }
      } ~
        path("actions" / Rest) { status =>
          {
            complete {
              soda.actions(Some(status))
            }
          }
        } ~
        path("views") {
          complete {
            soda.views(None, None, false)
          }
        } ~
        path("views" / Rest) { status =>
          {
            complete {
              soda.views(None, Some(status), false)
            }
          }
        } ~
        path("materialize" / Rest) { viewUrlPath =>
          {
            complete {
              soda.materialize(viewUrlPath)
            }
          }
        } ~
        path("command" / Rest) { commandId =>
          {
            complete {
              soda.commandStatus(commandId)
            }
          }
        } ~
        path("commands") {
          complete {
            soda.commands(None)
          }
        } ~
        path("commands" / Rest) { status =>
          {
            complete {
              soda.commands(Some(status))
            }
          }
        }
    }
  }

}
