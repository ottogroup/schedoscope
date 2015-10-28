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
package org.schedoscope.scheduler.api

import scala.language.postfixOps
import scala.language.implicitConversions
import scala.concurrent.duration.DurationInt
import org.schedoscope.scheduler.RootActor.settings
import org.schedoscope.scheduler.api.SchedoscopeJsonProtocol._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.io.IO
import akka.pattern.ask
import akka.routing.SmallestMailboxPool
import akka.util.Timeout
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import spray.routing.PathMatcher.PimpedPathMatcher
import spray.routing.directives.ParamDefMagnet.apply
import kamon.Kamon

object SchedoscopeRestService {
  Kamon.start
  val schedoscope = new SchedoscopeSystem()

  implicit val system = settings.system

  case class Config(shell: Boolean = false)

  val parser = new scopt.OptionParser[Config]("schedoscope-rest-service") {
    override def showUsageOnError = true
    head("schedoscope-rest-service", "0.0.1")
    help("help") text ("print usage")
    opt[Unit]('s', "shell") action { (_, c) => c.copy(shell = true) } optional () text ("start an interactive shell with direct schedoscope access.")
  }

  def main(args: Array[String]) {
    val config = parser.parse(args, Config()) match {
      case Some(config) => config
      case None         => Config()
    }
    start(config)
  }

  def start(config: Config) {

    val numActors = settings.restApiConcurrency
    val host = settings.host
    val port = settings.port
    val service = system.actorOf(new SmallestMailboxPool(numActors).props(Props(classOf[SchedoscopeRestServerActor], schedoscope)), "schedoscope-webservice-actor")
    implicit val timeout = Timeout(settings.webserviceTimeout)
    IO(Http) ? Http.Bind(service, interface = host, port = port)

    if (config.shell) {
      Thread.sleep(10000)
      println("\n\n============= schedoscope initialization finished ============== \n\n")
      SchedoscopeShell.start(schedoscope)
    }
  }

  def stop(): Boolean = {
    val schedoscopeTerminated = schedoscope.shutdown()

    system.shutdown()
    system.awaitTermination(5 seconds)
    system.actorSelection("/user/*").tell(PoisonPill, Actor.noSender)
    system.awaitTermination(5 seconds)
    Kamon.shutdown
    schedoscopeTerminated && system.isTerminated
  }
}

class SchedoscopeRestServerActor(schedoscope: SchedoscopeInterface) extends Actor with HttpService {

  def actorRefFactory = context

  def receive = runRoute(route)

  val route = get {
    respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
      parameters("status"?, "filter"?, "dependencies".as[Boolean]?, "typ"?, "mode" ?, "overview".as[Boolean] ?) { (status, filter, dependencies, typ, mode, overview) =>
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
              complete(schedoscope.views(viewUrlPath, status, filter, dependencies, overview))
            } ~
            path("materialize" / Rest ?) { viewUrlPath =>
              complete(schedoscope.materialize(viewUrlPath, status, filter, mode))
            } ~
            path("invalidate" / Rest ?) { viewUrlPath =>
              complete(schedoscope.invalidate(viewUrlPath, status, filter, dependencies))
            } ~
            path("newdata" / Rest ?) { viewUrlPath =>
              complete(schedoscope.newdata(viewUrlPath, status, filter))
            } ~
            path("command" / Rest) { commandId =>
              complete(schedoscope.commandStatus(commandId))
            } ~
            path("graph" / Rest) { viewUrlPath =>
              getFromFile(s"${settings.webResourcesDirectory}/graph.html")
            }
        }
      }
    }
  }
}

