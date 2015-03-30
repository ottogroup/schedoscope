package com.ottogroup.bi.soda.bottler.api

import scala.Array.canBuildFrom
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try
import com.bethecoder.ascii_table.ASCIITable
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser
import akka.actor.ActorSystem
import akka.event.Logging
import akka.util.Timeout
import spray.client.pipelining.Get
import spray.client.pipelining.WithTransformerConcatenation
import spray.client.pipelining.sendReceive
import spray.client.pipelining.unmarshal
import spray.httpx.SprayJsonSupport.sprayJsonUnmarshaller
import com.ottogroup.bi.soda.Settings

object CliFormat { // FIXME: a more generic parsing would be cool...
  def serialize(o: Any): String = {
    val sb = new StringBuilder()
    o match {
      case as: ActionStatusList => {
        sb.append(as.overview.map(el => s"${el._1} : ${el._2}").mkString("\n") + "\n")
        val header = Array("ACTOR", "STATUS", "STARTED", "DESC", "TARGET_VIEW", "PROPS")
        val running = as.actions.map(p => {
          val (s, d, t): (String, String, String) =
            if (p.runStatus.isDefined) {
              (p.runStatus.get.started.toString, p.runStatus.get.description, p.runStatus.get.targetView)
            } else {
              ("", "", "")
            }
          Array(p.actor, p.status, s, d, t, p.properties.mkString(","))
        }).toArray
        val queued = as.queues.flatMap(q => q._2.map(e => Array(s"${q._1}-queue", "queued", "no", q._2.toString, "", ""))).toArray
        sb.append(ASCIITable.getInstance.getTable(header, running ++ queued))
      }
      case vl: ViewStatusList => {
        sb.append(vl.overview.map(el => s"${el._1}: ${el._2}").mkString("\n") + "\n")
        if (!vl.views.isEmpty) {
          sb.append(s"Details:\n")
          val header = Array("VIEW", "STATUS", "PROPS")
          val data = vl.views.map(d => Array(d.view, d.status, d.properties.mkString(","))).toArray
          sb.append(ASCIITable.getInstance.getTable(header, data))
        }
      }
      case sc: SodaCommandStatus => {
        sb.append(s"id: ${sc.id}\n")
        sb.append(s"start: ${sc.start}\n")
        sb.append(s"status: ${sc.status}\n")
      }
      case f: Future[_] => {
        sb.append(s"submitted; isCompleted: ${f.isCompleted}\n")
      }
      case s: Seq[_] => {
        sb.append(s.map(el => serialize(el)).mkString("\n"))
      }
      case _ => sb.append(o)
    }
    sb.toString
  }
}

class SodaRestClient extends SodaInterface {

  var host = "localhost"
  var port = Settings().port

  import SodaJsonProtocol._

  implicit val system = ActorSystem("soda-spray-client")
  import system.dispatcher // execution context for futures below
  implicit val timeout = Timeout(10.days)
  val log = Logging(system, getClass)

  def get[T](q: String): Future[T] = {
    val pipeline = q match {
      case u: String if u.startsWith("/views") => sendReceive ~> unmarshal[ViewStatusList]
      case u: String if u.startsWith("/actions") => sendReceive ~> unmarshal[ActionStatusList]
      case u: String if u.startsWith("/materialize") => sendReceive ~> unmarshal[SodaCommandStatus]
      case u: String if u.startsWith("/commands") => sendReceive ~> unmarshal[List[SodaCommandStatus]]
      case _ => throw new RuntimeException("Unsupported query: " + q)
    }
    println("Calling Soda API URL: " + url(q))
    pipeline(Get(url(q))).asInstanceOf[Future[T]]
  }

  private def url(u: String) = {
    s"http://${host}:${port}${u}"
  }

  def shutdown() {
    system.shutdown()
  }

  def materialize(viewUrlPath: String): SodaCommandStatus = {
    Await.result(get[SodaCommandStatus](s"/materialize/${viewUrlPath}"), 10.days)
  }

  def invalidate(viewUrlPath: String): SodaCommandStatus = { null }

  def newdata(viewUrlPath: String): SodaCommandStatus = { null }

  def commandStatus(commandId: String): SodaCommandStatus = { null }

  def commands(status: Option[String]): List[SodaCommandStatus] = {
    val stat = if (status.isDefined) "/" + status else ""
    Await.result(get[List[SodaCommandStatus]](s"/commands${stat}"), 20.seconds)
  }

  def views(viewUrlPath: Option[String], status: Option[String], withDependencies: Boolean = false): ViewStatusList = {
    val stat = if (status.isDefined) "/" + status else ""
    Await.result(get[ViewStatusList](s"/views${stat}"), 20.seconds)
  }

  def actions(status: Option[String]): ActionStatusList = {
    val stat = if (status.isDefined) "/" + status else ""
    Await.result(get[ActionStatusList](s"/actions${stat}"), 20.seconds)
  }
}

object SodaClientControl {
  val soda = new SodaRestClient()
  val ctrl = new SodaControl(soda)
  def main(args: Array[String]) {
    ctrl.run(args)
    soda.shutdown()
    System.exit(0)
  }
}

class SodaControl(soda: SodaInterface) {
  object Action extends Enumeration {
    val VIEWS, ACTIONS, MATERIALIZE, COMMANDS = Value
  }
  import Action._

  case class Config(action: Option[Action.Value] = None, viewUrlPath: Option[String] = None, status: Option[String] = None)

  val parser = new scopt.OptionParser[Config]("soda-control") {
    override def showUsageOnError = true
    head("soda-control", "0.0.1")
    help("help") text ("print usage")
    cmd("views") action { (_, c) => c.copy(action = Some(VIEWS)) } text ("lists all view actors, along with their status") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter views by their status (e.g. 'transforming')"),
      opt[String]('v', "viewUrlPath") action { (x, c) => c.copy(viewUrlPath = Some(x)) } optional () valueName ("<viewUrlPath>") text ("view url path (e.g. 'my.database/MyView/Partition1/Partition2'). "))
    cmd("actions") action { (_, c) => c.copy(action = Some(ACTIONS)) } text ("list status of action actors") children ()
    cmd("commands") action { (_, c) => c.copy(action = Some(COMMANDS)) } text ("list commands") children ()
    cmd("materialize") action { (_, c) => c.copy(action = Some(MATERIALIZE)) } text ("materialize view(s)") children (
      opt[String]('v', "viewUrlPath") action { (x, c) => c.copy(viewUrlPath = Some(x)) } required () valueName ("<viewUrlPath>") text ("view url path (e.g. 'my.database/MyView/Partition1/Partition2'). "))
    checkConfig { c =>
      {
        if (!c.action.isDefined) failure("A command is required")
        else if (c.action.get.equals("materialize") && c.viewUrlPath.isDefined && Try(ViewUrlParser.parse(Settings().env, c.viewUrlPath.get)).isFailure) failure("Cannot parse view url path")
        else success
      }
    }
  }

  def run(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) => {
        println("Starting " + config.action.get.toString + " ...")
        try {
          val res = config.action.get match {
            case ACTIONS => {
              soda.actions(config.status)
            }
            case VIEWS => {
              soda.views(config.viewUrlPath, config.status, false)
            }
            case MATERIALIZE => {
              soda.materialize(config.viewUrlPath.get)
            }
            case COMMANDS => {
              soda.commands(None)
            }
            case _ => {
              println("Unsupported Action: " + config.action.get.toString)
            }
          }
          println("\nRESULTS\n=======")
          println(CliFormat.serialize(res))
        } catch {
          case t: Throwable => println(s"\nERROR: ${t.getMessage}\n")
        }
      }
      case None => // usage information has already been displayed
    }
  }

}



