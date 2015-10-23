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

import scala.concurrent.Future
import scala.util.Try
import com.bethecoder.ascii_table.ASCIITable
import org.schedoscope.Settings
import org.schedoscope.scheduler.messages._
import org.schedoscope.dsl.views.ViewUrlParser
import org.joda.time.format.DateTimeFormat

object CliFormat { // FIXME: a more generic parsing would be cool...

  def serialize(o: Any): String = {
    val sb = new StringBuilder()
    o match {
      case as: TransformationStatusList => {
        if (as.actions.size > 0) {
          val header = Array("ACTOR", "STATUS", "STARTED", "DESC", "TARGET_VIEW", "PROPS")
          val running = as.actions.map(p => {
            val (s, d, t): (String, String, String) =
              if (p.runStatus.isDefined) {
                (p.runStatus.get.started, p.runStatus.get.description, p.runStatus.get.targetView)
              } else {
                ("", "", "")
              }
            Array(p.actor, p.status, s, d, t, p.properties.mkString(","))
          }).toArray
          sb.append(ASCIITable.getInstance.getTable(header, running))
          sb.append(s"Total: ${running.size}\n")
        }
        sb.append("\n" + as.overview.map(el => s"${el._1} : ${el._2}").mkString("\n") + "\n")
      }

      case qs: QueueStatusList => {
        if (qs.queues.flatMap(q => q._2).size > 0) {
          val header = Array("TYP", "DESC", "TARGET_VIEW", "PROPS")
          val queued = qs.queues.flatMap(q => q._2.map(e => Array(q._1, e.description, e.targetView, e.properties.getOrElse("").toString))).toArray
          sb.append(ASCIITable.getInstance.getTable(header, queued))
          sb.append(s"Total: ${queued.size}")
        }
        sb.append("\n" + qs.overview.toSeq.sortBy(_._1).map(el => s"${el._1} : ${el._2}").mkString("\n") + "\n")
      }

      case vl: ViewStatusList => {
        if (!vl.views.isEmpty) {
          sb.append(s"Details:\n")
          val header = Array("VIEW", "STATUS", "PROPS")
          val data = vl.views.map(d => Array(d.view, d.status, d.properties.mkString(","))).toArray
          sb.append(ASCIITable.getInstance.getTable(header, data))
          sb.append(s"Total: ${data.size}\n")
        }
        sb.append("\n" + vl.overview.map(el => s"${el._1}: ${el._2}").mkString("\n") + "\n")
      }

      case sc: SchedoscopeCommandStatus => {
        sb.append(s"id: ${sc.id}\n")
        sb.append(s"start: ${sc.start}\n")
        sb.append(s"end: ${sc.end.getOrElse("")}\n")
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

object SchedoscopeClientControl extends App {
  val schedoscope = new SchedoscopeRestClient()
  val ctrl = new SchedoscopeControl(schedoscope)
  ctrl.run(args)
  schedoscope.shutdown()
  System.exit(0)
}

class SchedoscopeControl(schedoscope: SchedoscopeInterface) {
  object Action extends Enumeration {
    val VIEWS, TRANSFORMATIONS, QUEUES, MATERIALIZE, COMMANDS, INVALIDATE, NEWDATA, SHUTDOWN = Value
  }
  import Action._

  case class Config(action: Option[Action.Value] = None, viewUrlPath: Option[String] = None, status: Option[String] = None, typ: Option[String] = None, dependencies: Option[Boolean] = Some(false), filter: Option[String] = None, mode: Option[String] = None, overview: Option[Boolean] = None)

  val parser = new scopt.OptionParser[Config]("schedoscope-control") {
    override def showUsageOnError = true
    head("schedoscope-control", "0.0.1")
    help("help") text ("print usage")

    cmd("views") action { (_, c) => c.copy(action = Some(VIEWS)) } text ("lists all view actors, along with their status") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter views by their status (e.g. 'transforming')"),
      opt[String]('v', "viewUrlPath") action { (x, c) => c.copy(viewUrlPath = Some(x)) } optional () valueName ("<viewUrlPath>") text ("view url path (e.g. 'my.database/MyView/Partition1/Partition2'). "),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter view display (e.g. 'my.database/.*/Partition1/.*'). "),
      opt[Unit]('d', "dependencies") action { (_, c) => c.copy(dependencies = Some(true)) } optional () text ("include dependencies"),
      opt[Unit]('o', "overview") action { (_, c) => c.copy(overview = Some(true)) } optional () text ("show only overview, skip individual views"))

    cmd("transformations") action { (_, c) => c.copy(action = Some(TRANSFORMATIONS)) } text ("list status of running transformations") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter transformations by their status (e.g. 'queued, running, idle')"),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter transformation display (e.g. '.*hive-1.*'). "))

    cmd("queues") action { (_, c) => c.copy(action = Some(QUEUES)) } text ("list queued actions") children (
      opt[String]('t', "typ") action { (x, c) => c.copy(typ = Some(x)) } optional () valueName ("<type>") text ("filter queued actions by their type (e.g. 'oozie', 'filesystem', ...)"),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter queued actions (e.g. '.*my.dabatase/myView.*'). "))

    cmd("commands") action { (_, c) => c.copy(action = Some(COMMANDS)) } text ("list commands") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter commands by their status (e.g. 'failed')"),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter command display (e.g. '.*201501.*'). "))

    cmd("materialize") action { (_, c) => c.copy(action = Some(MATERIALIZE)) } text ("materialize view(s)") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter views to be materialized by their status (e.g. 'failed')"),
      opt[String]('v', "viewUrlPath") action { (x, c) => c.copy(viewUrlPath = Some(x)) } optional () valueName ("<viewUrlPath>") text ("view url path (e.g. 'my.database/MyView/Partition1/Partition2'). "),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter views to be materialized (e.g. 'my.database/.*/Partition1/.*'). "),
      opt[String]('m', "mode") action { (x, c) => c.copy(mode = Some(x)) } optional () valueName ("<mode>") text ("materialization mode. Supported modes are currently 'RESET_TRANSFORMATION_CHECKSUMS, RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS'"))

    cmd("invalidate") action { (_, c) => c.copy(action = Some(INVALIDATE)) } text ("invalidate view(s)") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter views to be invalidated by their status (e.g. 'transforming')"),
      opt[String]('v', "viewUrlPath") action { (x, c) => c.copy(viewUrlPath = Some(x)) } optional () valueName ("<viewUrlPath>") text ("view url path (e.g. 'my.database/MyView/Partition1/Partition2'). "),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter views to be invalidated (e.g. 'my.database/.*/Partition1/.*'). "),
      opt[Unit]('d', "dependencies") action { (_, c) => c.copy(dependencies = Some(true)) } optional () text ("invalidate dependencies as well"))

    cmd("newdata") action { (_, c) => c.copy(action = Some(NEWDATA)) } text ("send newdata") children (
      opt[String]('s', "status") action { (x, c) => c.copy(status = Some(x)) } optional () valueName ("<status>") text ("filter views to send 'newdata' to by their status (e.g. 'failed')"),
      opt[String]('v', "viewUrlPath") action { (x, c) => c.copy(viewUrlPath = Some(x)) } optional () valueName ("<viewUrlPath>") text ("view url path (e.g. 'my.database/MyView/Partition1/Partition2'). "),
      opt[String]('f', "filter") action { (x, c) => c.copy(filter = Some(x)) } optional () valueName ("<regex>") text ("regular expression to filter views to send 'newdata' to (e.g. 'my.database/.*/Partition1/.*'). "))

    cmd("shutdown") action { (_, c) => c.copy(action = Some(SHUTDOWN)) } text ("shutdown program")

    checkConfig { c =>
      if (!c.action.isDefined) failure("A command is required")
      else if (c.action.get == MATERIALIZE && c.mode.isDefined && !MaterializeViewMode.values.map { _.toString }.contains(c.mode.get)) failure(s"mode ${c.mode.get} not supported. Supported are: '${MaterializeViewMode.values.map(_.toString())}'")
      else success
    }
  }

  def run(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) => {
        println("Starting " + config.action.get.toString + " ...")
        try {
          val res = config.action.get match {
            case TRANSFORMATIONS => {
              schedoscope.transformations(config.status, config.filter)
            }
            case QUEUES => {
              schedoscope.queues(config.typ, config.filter)
            }
            case VIEWS => {
              schedoscope.views(config.viewUrlPath, config.status, config.filter, config.dependencies, config.overview)
            }
            case MATERIALIZE => {
              schedoscope.materialize(config.viewUrlPath, config.status, config.filter, config.mode)
            }
            case INVALIDATE => {
              schedoscope.invalidate(config.viewUrlPath, config.status, config.filter, config.dependencies)
            }
            case NEWDATA => {
              schedoscope.newdata(config.viewUrlPath, config.status, config.filter)
            }
            case COMMANDS => {
              schedoscope.commands(config.status, config.filter)
            }
            case SHUTDOWN => {
              schedoscope.shutdown()
              System.exit(0)
            }
            case _ => {
              println("Unsupported Action: " + config.action.get.toString)
            }
          }
          println("\nRESULTS\n=======")
          println(CliFormat.serialize(res))
        } catch {
          case t: Throwable => println(s"\nERROR: ${t.getMessage}\n"); t.printStackTrace()
        }
      }
      case None => // usage information has already been displayed
    }
  }

}
