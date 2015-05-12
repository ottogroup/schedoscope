package org.schedoscope.scheduler.api

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import org.schedoscope.Settings
import org.schedoscope.scheduler.SchedoscopeRootActor.settings

import akka.actor.Actor
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.io.Inet
import akka.io.Tcp
import akka.pattern.ask
import akka.routing.RoundRobinRouter
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import spray.can.Http
import spray.can.server.ServerSettings
import spray.http.HttpHeaders.RawHeader
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.io.ServerSSLEngineProvider
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import spray.routing.PathMatcher.PimpedPathMatcher
import spray.routing.Route
import spray.routing.directives.ParamDefMagnet.apply

trait SimpleParallelRoutingApp extends HttpService {

  @volatile private[this] var _refFactory: Option[ActorRefFactory] = None

  import org.schedoscope.scheduler.SchedoscopeRootActor.settings

  import SchedoscopeJsonProtocol._

  implicit def actorRefFactory = _refFactory getOrElse sys.error(
    "Route creation is not fully supported before `startServer` has been called, " +
      "maybe you can turn your route definition into a `def` ?")

  def startSchedoscope(implicit system: ActorSystem, schedoscope: SchedoscopeSystem) = {
    startServer(interface = "localhost", port = settings.port) {
      get {
        respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
          parameters("status"?, "filter"?, "dependencies".as[Boolean]?, "typ"?, "mode" ?, "overview".as[Boolean] ?) { (status, filter, dependencies, typ, mode, overview) =>
            {
              path("actions") {
                complete(schedoscope.actions(status, filter))
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

  }

  def startServer(interface: String,
    port: Int,
    serviceActorName: String = "simple-service-actor",
    backlog: Int = 100,
    options: immutable.Traversable[Inet.SocketOption] = Nil,
    settings: Option[ServerSettings] = None)(route: ⇒ Route)(implicit system: ActorSystem, sslEngineProvider: ServerSSLEngineProvider,
      bindingTimeout: Timeout = 1.second): Future[Http.Bound] = {
    
    val serviceActor = system.actorOf(
      props = Props {
        new Actor {
          _refFactory = Some(context)
          def receive = {
            val system = 0 
            runRoute(route)
          }
        }
      }.withRouter(RoundRobinRouter(nrOfInstances = Settings().restApiConcurrency)),
      name = serviceActorName)
      
    IO(Http).ask(Http.Bind(serviceActor, interface, port, backlog, options, settings)).flatMap {
      case b: Http.Bound ⇒ Future.successful(b)
      case Tcp.CommandFailed(b: Http.Bind) ⇒
        Future.failed(new RuntimeException(
          "Binding failed. Switch on DEBUG-level logging for `akka.io.TcpListener` to log the cause."))
    }(system.dispatcher)
  }
}