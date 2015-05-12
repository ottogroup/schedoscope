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

import org.schedoscope.scheduler.SchedoscopeRootActor.settings;

object SchedoscopeService extends App with SimpleParallelRoutingApp {

  case class Config(shell: Boolean = true)

  val parser = new scopt.OptionParser[Config]("schedoscope-service") {
    override def showUsageOnError = true
    head("schedoscope-service", "0.0.1")
    help("help") text ("print usage")
  }

  val config = parser.parse(args, Config()) match {
    case Some(config) => config
    case None => Config()
  }

  val schedoscope = new SchedoscopeSystem()

  implicit val system = ActorSystem("schedoscope-webservice")

  startSchedoscope(system, schedoscope)

  Thread.sleep(10000)
  println("\n\n============= SODA initialization finished ============== \n\n")
  val ctrl = new SchedoscopeControl(schedoscope)
  val reader = new ConsoleReader()
  val history = new History()

  history.setHistoryFile(new File(System.getenv("HOME") + "/.schedoscope_history"))
  reader.setHistory(history)
  while (true) {
    try {
      val cmd = reader.readLine("schedoscope> ")
      // we have to intercept --help because otherwise jline seems to call System.exit :(
      if (cmd != null && !cmd.trim().replaceAll(";", "").isEmpty() && !cmd.matches(".*--help.*"))
        ctrl.run(cmd.split("\\s+"))
    } catch {
      case t: Throwable => println(s"ERROR: ${t.getMessage}\n\n"); t.printStackTrace()
    }
  }

}
