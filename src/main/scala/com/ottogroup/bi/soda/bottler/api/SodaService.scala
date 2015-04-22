package com.ottogroup.bi.soda.bottler.api

import com.ottogroup.bi.soda.bottler.SodaRootActor.settings
import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import akka.util.Timeout
import scala.concurrent.duration._
import jline.ConsoleReader
import spray.http.HttpHeaders.RawHeader

object SodaService extends App with SimpleParallelRoutingApp {

  val soda = new SodaSystem()

  implicit val system = ActorSystem("soda-webservice")

  import SodaJsonProtocol._

  startServer(interface = "localhost", port = settings.port) {
    get {
      respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
        path("actions") {
          parameters("status"?, "filter"?) { (status, filter) =>
            complete(soda.actions(status, filter))
          }
        } ~
          path("commands") {
            parameters("status"?, "filter"?) { (status, filter) =>
              complete(soda.commands(status, filter))
            }
          } ~
          path("views" / Rest ?) { viewUrlPath =>
            parameters("status"?, "filter"?, "dependencies".as[Boolean]?) { (status, filter, dependencies) =>
              complete(soda.views(viewUrlPath, status, filter, dependencies))
            }
          } ~
          path("materialize" / Rest) { viewUrlPath =>
            complete(soda.materialize(viewUrlPath))
          } ~
          path("invalidate" / Rest) { viewUrlPath =>
            complete(soda.invalidate(viewUrlPath))
          } ~
          path("newdata" / Rest) { viewUrlPath =>
            complete(soda.newdata(viewUrlPath))
          } ~
          path("command" / Rest) { commandId =>
            complete(soda.commandStatus(commandId))
          } ~
          path("graph" / Rest) { viewUrlPath =>
            getFromFile(s"${settings.webResourcesDirectory}/graph.html")
          }
      }
    }
  }

  Thread.sleep(10000)
  println("\n\n============= SODA initialization finished ============== \n\n")
  val ctrl = new SodaControl(soda)
  val reader = new ConsoleReader()
  while (true) {
    try {
      val cmd = reader.readLine("soda> ")
      if (cmd != null && !cmd.trim().replaceAll(";", "").isEmpty())
        ctrl.run(cmd.split("\\s+"))
    } catch {
      case t: Throwable => println(s"ERROR: ${t.getMessage}\n\n"); t.printStackTrace()
    }
  }

}
