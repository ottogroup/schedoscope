package org.schedoscope.scheduler.api

import akka.actor.ActorSystem
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import akka.util.Timeout
import scala.concurrent.duration._
import jline.ConsoleReader
import spray.http.HttpHeaders.RawHeader
import jline.History

import java.io.File

import org.apache.commons.daemon._
import org.schedoscope.scheduler.SchedoscopeRootActor.settings;

import akka.actor.PoisonPill
import akka.actor.Actor

trait ApplicationLifecycle {
  def start(): Unit
  def stop(): Unit
}

abstract class AbstractApplicationDaemon extends Daemon {
  def application: ApplicationLifecycle

  def init(daemonContext: DaemonContext) {}

  def start() = application.start()

  def stop() = application.stop()

  def destroy() = application.stop()
}

class ApplicationDaemon() extends AbstractApplicationDaemon {
  def application = new SchedosopeDaemon
}

object SchedosopeDaemon extends App {
  val application = createApplication()

  def createApplication() = new ApplicationDaemon

  private[this] var cleanupAlreadyRun: Boolean = false

  def cleanup() {
    val previouslyRun = cleanupAlreadyRun
    cleanupAlreadyRun = true
    if (!previouslyRun) application.stop()
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      cleanup()
    }
  }))

  application.start()
}

class SchedosopeDaemon extends ApplicationLifecycle with SimpleParallelRoutingApp {
  val schedoscope = new SchedoscopeSystem()

  implicit val system = ActorSystem("schedoscope-webservice")

  import SchedoscopeJsonProtocol._
  def init(context: String): Unit = {}
  def init(context: DaemonContext) = {}

  def start() = {
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

  def stop() {
    system.shutdown()
    system.awaitTermination(5 seconds)
    system.actorSelection("/user/*").tell(PoisonPill, Actor.noSender)
    system.awaitTermination(5 seconds)
    if (system.isTerminated)
      System.exit(0)
    else
      System.exit(1)
  }

}
