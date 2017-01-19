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

import java.util.logging.{Level, LogManager, Logger}

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.routing.SmallestMailboxPool
import akka.util.Timeout
import org.schedoscope.Schedoscope
import org.schedoscope.scheduler.commandline.SchedoscopeCliRepl
import org.schedoscope.scheduler.rest.SchedoscopeJsonDataFormat
import org.schedoscope.scheduler.service._
import org.slf4j.bridge.SLF4JBridgeHandler
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http.StatusCodes._
import spray.routing.Directive.pimpApply
import spray.routing.{ExceptionHandler, HttpService, Route}
import spray.util.LoggingContext

import scala.concurrent.ExecutionContextExecutor
import scala.language.postfixOps


/**
  * Spray actor providing the Schedoscope REST service
  */
class SchedoscopeRestServiceActor(schedoscope: SchedoscopeService) extends Actor with HttpService {

  import spray.httpx.SprayJsonSupport._
  import SchedoscopeJsonDataFormat._

  implicit def executionContext: ExecutionContextExecutor = actorRefFactory.dispatcher

  implicit def restExceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e: IllegalArgumentException =>
        requestUri { uri =>
          log.warning("Invalid scheduling request: {} Problem: {}", uri, e)
          complete(BadRequest, e.getMessage)
        }

      case t: Throwable =>
        requestUri { uri =>
          log.warning("General exception caught while processing scheduling request: {} Problem: {}", uri, t)
          complete(InternalServerError, s"Server encountered exception: $t")
        }
    }

  override def actorRefFactory: ActorContext = context

  override def receive: Receive = runRoute(get {
    respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
      parameters("status" ?, "filter" ?, "issueFilter" ?, "dependencies".as[Boolean] ?, "typ" ?, "mode" ?, "overview".as[Boolean] ?, "all".as[Boolean] ?) {
        (status, filter, issueFilter, dependencies, typ, mode, overview, all) =>
          path("transformations") {
            complete(schedoscope.transformations(status, filter))
          } ~
          /*
          path("queues") {
            complete(schedoscope.queues(typ, filter))
          } ~ */
          path("views" / Rest.?) { viewUrlPath =>
            complete(schedoscope.views(viewUrlPath, status, filter, issueFilter, dependencies, overview, all))
          } ~
          path("materialize" / Rest.?) { viewUrlPath =>
            complete(schedoscope.materialize(viewUrlPath, status, filter, issueFilter, mode))
          } ~
          path("invalidate" / Rest.?) { viewUrlPath =>
            complete(schedoscope.invalidate(viewUrlPath, status, filter, issueFilter, dependencies))
          }
      }
    }
  })
}

/**
  * Main object for launching the schedoscope rest service.
  */
object SchedoscopeRestService {

  LogManager.getLogManager.reset()
  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()
  Logger.getLogger("global").setLevel(Level.FINEST)

  import Schedoscope._

  implicit val actorSystem: ActorSystem = Schedoscope.actorSystem
  implicit val timeout: Timeout = Timeout(settings.webserviceTimeout)

  val schedoscope: SchedoscopeService = new SchedoscopeServiceImpl(actorSystem, settings, viewManagerActor, transformationManagerActor)
  val service: ActorRef = actorSystem.actorOf(new SmallestMailboxPool(settings.restApiConcurrency).props(Props(classOf[SchedoscopeRestServiceActor], schedoscope)), "schedoscope-webservice-actor")

  def start(config: Config) {
    IO(Http) ? Http.Bind(service, interface = settings.host, port = settings.port)

    if (config.shell) {
      Thread.sleep(5000)
      println("\n\n============= schedoscope initialization finished ============== \n\n")
      new SchedoscopeCliRepl(schedoscope).start()
    }
  }

  def stop(): Boolean = {
    schedoscope.shutdown()
  }

  case class Config(shell: Boolean = false)

  val parser = new scopt.OptionParser[Config]("schedoscope-rest-service") {
    override def showUsageOnError = true

    head("schedoscope-rest-service")
    help("help") text "print usage"
    opt[Unit]('s', "shell") action { (_, c) => c.copy(shell = true) } optional() text "start an interactive shell with direct schedoscope access."
  }

  def main(args: Array[String]) {
    start(parser.parse(args, Config()) match {
      case Some(config) => config
      case None => Config()
    })
  }
}

