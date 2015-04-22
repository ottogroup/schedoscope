package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable
import akka.actor.{ ActorSystem, ActorRefFactory, Actor, Props }
import akka.pattern.ask
import akka.util.Timeout
import akka.io.{ Inet, IO, Tcp }
import spray.io.ServerSSLEngineProvider
import spray.can.Http
import spray.can.server.ServerSettings
import spray.routing.HttpService
import spray.routing.Route
import akka.routing.RoundRobinRouter
import com.ottogroup.bi.soda.Settings

trait SimpleParallelRoutingApp extends HttpService {

  @volatile private[this] var _refFactory: Option[ActorRefFactory] = None

  implicit def actorRefFactory = _refFactory getOrElse sys.error(
    "Route creation is not fully supported before `startServer` has been called, " +
      "maybe you can turn your route definition into a `def` ?")

  /**
   * Starts a new spray-can HTTP server with a default HttpService for the given route and binds the server to the
   * given interface and port.
   * The method returns a Future on the Bound event returned by the HttpListener as a reply to the Bind command.
   * You can use the Future to determine when the server is actually up (or you can simply drop it if you are not
   * interested in it).
   */
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
            val system = 0 // shadow implicit system
            runRoute(route)
          }
        }
      }.withRouter(RoundRobinRouter(nrOfInstances = Settings().restApiConcurrency)),
      name = serviceActorName)
    IO(Http).ask(Http.Bind(serviceActor, interface, port, backlog, options, settings)).flatMap {
      case b: Http.Bound ⇒ Future.successful(b)
      case Tcp.CommandFailed(b: Http.Bind) ⇒
        // TODO: replace by actual exception when Akka #3861 is fixed.
        //       see https://www.assembla.com/spaces/akka/tickets/3861
        Future.failed(new RuntimeException(
          "Binding failed. Switch on DEBUG-level logging for `akka.io.TcpListener` to log the cause."))
    }(system.dispatcher)
  }
}