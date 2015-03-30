package com.ottogroup.bi.soda.bottler.api

import com.ottogroup.bi.soda.bottler.SodaRootActor.settings

import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import akka.util.Timeout
import scala.concurrent.duration._

object SodaService extends App with SimpleRoutingApp {

  implicit val system = ActorSystem("soda-webservice")
  implicit val timeout = Timeout(600.seconds)
  val soda = new SodaSystem()

  import SodaJsonProtocol._

  startServer(interface = "localhost", port = settings.port) {
    get {
      path("actions") {
        parameters("status"?) { status =>
          complete(soda.actions(status))
        }
      } ~
        path("commands") {
          parameters("status"?) { status =>
            complete(soda.commands(status))
          }
        }
      path("views" / Rest ?) { viewUrlPath =>
        parameters("status" ?, "dependencies".as[Boolean] ? false) { (status, withDependencies) =>
          complete(soda.views(viewUrlPath, status, withDependencies))
        }
      } ~
        path("materialize" / Rest) { viewUrlPath =>
          complete(soda.materialize(viewUrlPath))
        } ~
        path("command" / Rest) { commandId =>
          complete(soda.commandStatus(commandId))
        }
    }
  }

  Thread.sleep(10000)
  println("\n\n============= SODA initialization finished ============== \n\n")
  val ctrl = new SodaControl(soda)
  while (true) {
    try {
      val cmd = readLine("soda> ")
      if (!cmd.trim().replaceAll(";", "").isEmpty())
        ctrl.run(cmd.split("\\s+"))
    } catch {
      case t: Throwable => println(s"ERROR: ${t.getMessage}\n\n"); t.printStackTrace()
    }
  }

}
