package com.ottogroup.bi.soda.bottler.api

import scala.concurrent.duration.DurationInt

import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.bottler.SodaRootActor.settings
import com.ottogroup.bi.soda.bottler.api.SodaJsonProtocol._

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

object SodaRestService {
  
  val soda = new SodaSystem()
  
  implicit val system = ActorSystem("soda-rest-service")

  case class Config(shell: Boolean = false)

  val parser = new scopt.OptionParser[Config]("soda-rest-service") {
    override def showUsageOnError = true
    head("soda-rest-service", "0.0.1")
    help("help") text ("print usage")
    opt[Unit]('s', "shell") action { (_, c) => c.copy(shell = true) } optional () text ("start an interactive shell with direct soda access.")
  }

  def main(args: Array[String]) {
    val config = parser.parse(args, Config()) match {
      case Some(config) => config
      case None         => Config()
    }
    start(config)
  }

  def start(config: Config) {
    val numActors = Settings().restApiConcurrency
    val host = Settings().host
    val port = Settings().port
    val service = system.actorOf(new SmallestMailboxPool(numActors).props(Props(classOf[SodaRestServerActor], soda)), "soda-webservice-actor")
    implicit val timeout = Timeout(Settings().webserviceTimeOut.length)
    IO(Http) ? Http.Bind(service, interface = host, port = port)

    if (config.shell) {
      Thread.sleep(10000)
      println("\n\n============= SODA initialization finished ============== \n\n")
      SodaShell.start(soda)
    }
  }
  
  def stop() : Boolean = {
    val sodaTerminated = soda.shutdown()
    system.shutdown()
    system.awaitTermination(5 seconds)
    system.actorSelection("/user/*").tell(PoisonPill, Actor.noSender)
    system.awaitTermination(5 seconds)  
    sodaTerminated && system.isTerminated
  }
}

class SodaRestServerActor(soda: SodaInterface) extends Actor with HttpService {

  def actorRefFactory = context

  def receive = runRoute(route)

  val route = get {
    respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
      parameters("status"?, "filter"?, "dependencies".as[Boolean]?, "typ"?, "mode" ?, "overview".as[Boolean] ?) { (status, filter, dependencies, typ, mode, overview) =>
        {
          path("actions") {
            complete(soda.actions(status, filter))
          } ~
            path("queues") {
              complete(soda.queues(typ, filter))
            } ~
            path("commands") {
              complete(soda.commands(status, filter))
            } ~
            path("views" / Rest ?) { viewUrlPath =>
              complete(soda.views(viewUrlPath, status, filter, dependencies, overview))
            } ~
            path("materialize" / Rest ?) { viewUrlPath =>
              complete(soda.materialize(viewUrlPath, status, filter, mode))
            } ~
            path("invalidate" / Rest ?) { viewUrlPath =>
              complete(soda.invalidate(viewUrlPath, status, filter, dependencies))
            } ~
            path("newdata" / Rest ?) { viewUrlPath =>
              complete(soda.newdata(viewUrlPath, status, filter))
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
  }
}


