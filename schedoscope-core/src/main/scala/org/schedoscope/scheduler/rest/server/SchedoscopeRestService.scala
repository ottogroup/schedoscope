/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.rest.server

import java.util.logging.{ Level, LogManager, Logger }

import akka.actor.{ Actor, Props }
import akka.io.IO
import akka.pattern.ask
import akka.routing.SmallestMailboxPool
import akka.util.Timeout
import org.schedoscope.Schedoscope
import org.schedoscope.scheduler.commandline.SchedoscopeCliRepl
import org.schedoscope.scheduler.rest.SchedoscopeJsonDataFormat._
import org.schedoscope.scheduler.service.{ SchedoscopeService, SchedoscopeServiceImpl }
import org.slf4j.bridge.SLF4JBridgeHandler
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

import scala.language.postfixOps

/**
 * Spray actor providing the Schedoscope REST service
 */
class SchedoscopeRestServiceActor(schedoscope: SchedoscopeService) extends Actor with HttpService {
  def actorRefFactory = context

  def receive = runRoute(route)

  val route = get {
    respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
      parameters("status"?, "filter"?, "dependencies".as[Boolean]?, "typ"?, "mode" ?, "overview".as[Boolean] ?, "all".as[Boolean] ?) { (status, filter, dependencies, typ, mode, overview, all) =>
        {
          path("transformations") {
            complete(schedoscope.transformations(status, filter))
          } ~
            path("queues") {
              complete(schedoscope.queues(typ, filter))
            } ~
            path("commands") {
              complete(schedoscope.commands(status, filter))
            } ~
            path("views" / Rest ?) { viewUrlPath =>
              complete(schedoscope.views(viewUrlPath, status, filter, dependencies, overview, all))
            } ~
            path("materialize" / Rest ?) { viewUrlPath =>
              complete(schedoscope.materialize(viewUrlPath, status, filter, mode))
            } ~
            path("invalidate" / Rest ?) { viewUrlPath =>
              complete(schedoscope.invalidate(viewUrlPath, status, filter, dependencies))
            } ~
            path("command" / Rest) { commandId =>
              complete(schedoscope.commandStatus(commandId))
            }
        }
      }
    }
  }
}

/**
 * Main object for launching the schedoscope rest service.
 */
object SchedoscopeRestService {
  implicit val actorSystem = Schedoscope.actorSystem
  val settings = Schedoscope.settings
  val viewManagerActor = Schedoscope.viewManagerActor
  val transactionManagerActor = Schedoscope.transformationManagerActor

  LogManager.getLogManager().reset()
  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()
  Logger.getLogger("global").setLevel(Level.FINEST)

  val schedoscope = new SchedoscopeServiceImpl(actorSystem, settings, viewManagerActor, transactionManagerActor)

  case class Config(shell: Boolean = false)

  val parser = new scopt.OptionParser[Config]("schedoscope-rest-service") {
    override def showUsageOnError = true

    head("schedoscope-rest-service", "0.0.1")
    help("help") text ("print usage")
    opt[Unit]('s', "shell") action { (_, c) => c.copy(shell = true) } optional () text ("start an interactive shell with direct schedoscope access.")
  }

  def start(config: Config) {

    val numActors = settings.restApiConcurrency
    val host = settings.host
    val port = settings.port
    val service = actorSystem.actorOf(new SmallestMailboxPool(numActors).props(Props(classOf[SchedoscopeRestServiceActor], schedoscope)), "schedoscope-webservice-actor")
    implicit val timeout = Timeout(settings.webserviceTimeout)
    IO(Http) ? Http.Bind(service, interface = host, port = port)

    if (config.shell) {
      Thread.sleep(5000)
      println("\n\n============= schedoscope initialization finished ============== \n\n")
      new SchedoscopeCliRepl(schedoscope).start()
    }
  }

  def stop(): Boolean = {
    schedoscope.shutdown()
  }

  def main(args: Array[String]) {
    val config = parser.parse(args, Config()) match {
      case Some(config) => config
      case None         => Config()
    }
    start(config)
  }
}

