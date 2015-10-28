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
package org.schedoscope.scheduler.service

import scala.language.postfixOps
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.driver.DriverRunState
import org.schedoscope.scheduler.driver.DriverRunFailed
import org.schedoscope.scheduler.driver.DriverRunOngoing
import org.schedoscope.scheduler.driver.DriverRunSucceeded
import org.schedoscope.dsl.transformations.Transformation
import org.schedoscope.scheduler.actors.ViewManagerActor
import org.schedoscope.dsl.Named
import org.schedoscope.dsl.View
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.event.Logging
import org.schedoscope.AskPattern._
import akka.pattern.Patterns
import akka.util.Timeout
import akka.actor.ActorSystem
import org.schedoscope.SettingsImpl
import org.schedoscope.scheduler.actors.ViewManagerActor
import scala.concurrent.Future


class SchedoscopeServiceImpl(actorSystem: ActorSystem, settings: SettingsImpl, viewManagerActor: ActorRef, transformationManagerActor: ActorRef) extends SchedoscopeService {
  val log = Logging(actorSystem, classOf[ViewManagerActor])

  transformationManagerActor ! DeployCommand()

  case class SchedoscopeCommand(id: String, start: String, parts: List[Future[_]])
  val runningCommands = collection.mutable.HashMap[String, SchedoscopeCommand]()
  val doneCommands = collection.mutable.HashMap[String, SchedoscopeCommandStatus]()

  private def viewsFromUrl(viewUrlPath: String) = {
    View.viewsFromUrl(settings.env, viewUrlPath, settings.viewAugmentor)
  }

  private def getViews(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Boolean = false) = {
    val resolvedViews = if (viewUrlPath.isDefined && !viewUrlPath.get.isEmpty()) Some(viewsFromUrl(viewUrlPath.get)) else None
    queryActor[ViewStatusListResponse](viewManagerActor, GetViews(resolvedViews, status, filter, dependencies), settings.viewManagerResponseTimeout).viewStatusList
  }

  private def commandId(command: Any, args: Seq[Option[String]], start: Option[LocalDateTime] = None) = {
    val format = DateTimeFormat.forPattern("YYYYMMddHHmmss");
    val c = command match {
      case s: String => s
      case c: Any    => Named.camelToLowerUnderscore(c.getClass.getSimpleName)
    }
    val a = if (args.size == 0) "_" else args.filter(_.isDefined).map(_.getOrElse("_")).mkString(":")
    if (start.isDefined) {
      val s = format.print(start.get)
      s"${c}::${a}::${s}"
    } else {
      s"${c}::${a}"
    }
  }

  private def submitCommandInternal(actors: List[ActorRef], command: Any, args: Option[String]*) = {
    val currentCommandId = runningCommandId(command.toString, args: _*)
    if (currentCommandId.isDefined) {
      commandStatus(currentCommandId.get)
    } else {
      val jobFutures = actors.map { actor => Patterns.ask(actor, command, Timeout(settings.completitionTimeout)) }
      val start = new LocalDateTime()
      val id = commandId(command, args, Some(start))
      runningCommands.put(id, SchedoscopeCommand(id, formatDate(start), jobFutures))
      SchedoscopeCommandStatus(id, formatDate(start), None, Map("submitted" -> jobFutures.size))
    }
  }

  private def finalizeInternal(commandId: String, start: String, status: Map[String, Int]) = {
    runningCommands.remove(commandId)
    val scs = SchedoscopeCommandStatus(commandId, start, Some(formatDate(new LocalDateTime())), status)
    doneCommands.put(commandId, scs)
    scs
  }

  private def runningCommandId(command: String, args: Option[String]*): Option[String] = {
    val cidPrefix = commandId(command, args)
    runningCommands.keys
      .foreach(k =>
        if (k.startsWith(cidPrefix))
          return Some(k))
    None
  }

  private def formatDate(d: LocalDateTime): String = {
    if (d != null) DateTimeFormat.shortDateTime().print(d) else ""
  }

  private def parseActionStatus(a: TransformationStatusResponse[_]): TransformationStatus = {
    val actor = getOrElse(a.actor.path.toStringWithoutAddress, "unknown")
    val typ = if (a.driver != null) getOrElse(a.driver.transformationName, "unknown") else "unknown"
    var drh = a.driverRunHandle
    var status = if (a.message != null) a.message else "none"
    var comment = ""

    if (a.driverRunStatus != null) {
      a.driverRunStatus.asInstanceOf[DriverRunState[Any with Transformation]] match {
        case s: DriverRunSucceeded[_] => { comment = getOrElse(s.comment, "no-comment"); status = "succeeded" }
        case f: DriverRunFailed[_]    => { comment = getOrElse(f.reason, "no-reason"); status = "failed" }
        case o: DriverRunOngoing[_]   => { drh = o.runHandle }
      }
    }

    if (drh != null) {
      val desc = drh.transformation.asInstanceOf[Transformation].description
      val view = drh.transformation.asInstanceOf[Transformation].getView()
      val started = drh.started
      val runStatus = RunStatus(getOrElse(desc, "no-desc"), getOrElse(view, "no-view"), getOrElse(formatDate(started), ""), comment, None)
      TransformationStatus(actor, typ, status, Some(runStatus), None)
    } else {
      TransformationStatus(actor, typ, status, None, None)
    }
  }

  private def parseQueueElements(q: List[AnyRef]): List[RunStatus] = {
    q.map(o =>
      if (o.isInstanceOf[Transformation]) {
        val trans = o.asInstanceOf[Transformation]
        RunStatus(trans.description, trans.getView(), "", "", None)
      } else {
        RunStatus(o.toString, "", "", "", None)
      })
  }

  private def getOrElse[T](o: T, d: T) = {
    if (o != null) o else d;
  }

  def materialize(viewUrlPath: Option[String], status: Option[String], filter: Option[String], mode: Option[String]) = {
    val viewActors = getViews(viewUrlPath, status, filter).map(v => v.actor)
    submitCommandInternal(viewActors, MaterializeView(
      try {
        MaterializeViewMode.withName(mode.getOrElse("DEFAULT"))
      } catch {
        case _: NoSuchElementException => MaterializeViewMode.DEFAULT
      }), viewUrlPath, status, filter)
  }

  def invalidate(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean]) = {
    val viewActors = getViews(viewUrlPath, status, filter, dependencies.getOrElse(false)).map(v => v.actor)
    submitCommandInternal(viewActors, Invalidate(), viewUrlPath, status, filter)
  }

  def newdata(viewUrlPath: Option[String], status: Option[String], filter: Option[String]) = {
    val viewActors = getViews(viewUrlPath, status, filter).map(v => v.actor)
    submitCommandInternal(viewActors, "newdata", viewUrlPath, status, filter)
  }

  def views(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean], overview: Option[Boolean]) = {
    val views = getViews(viewUrlPath, status, filter, dependencies.getOrElse(false))
    val statusMap = views.map(v => (v.view.urlPath, v.status)).toMap // needed for status of dependencies in output
    val viewStatusList = views
      .map(v => ViewStatus(v.view.urlPath, v.status, None, if (!dependencies.getOrElse(false)) None else Some(v.view.dependencies.map(d => ViewStatus(d.urlPath, statusMap.getOrElse(d.urlPath, ""), None, None)).toList)))
    val ov = viewStatusList.groupBy(_.status).mapValues(_.size)
    ViewStatusList(ov, if (overview.getOrElse(false)) List() else viewStatusList)
  }

  def transformations(status: Option[String], filter: Option[String]) = {
    val result = queryActor[TransformationStatusListResponse](transformationManagerActor, GetTransformations(), settings.statusListAggregationTimeout)
    val actions = result.transformationStatusList
      .map(a => parseActionStatus(a))
      .filter(a => !status.isDefined || status.get.equals(a.status))
      .filter(a => !filter.isDefined || a.actor.matches(filter.get)) // FIXME: is the actor name a good filter criterion?
    val overview = actions
      .groupBy(_.status)
      .map(el => (el._1, el._2.size))
    TransformationStatusList(overview, actions)
  }

  def queues(typ: Option[String], filter: Option[String]): QueueStatusList = {
    val result = queryActor[QueueStatusListResponse](transformationManagerActor, GetQueues(), settings.statusListAggregationTimeout)
    val queues = result.transformationQueues
      .filterKeys(t => !typ.isDefined || t.startsWith(typ.get))
      .map { case (t, queue) => (t, parseQueueElements(queue)) }
      .map { case (t, queue) => (t, queue.filter(el => !filter.isDefined || el.targetView.matches(filter.get))) }
    val overview = queues
      .map(el => (el._1, el._2.size))
    QueueStatusList(overview, queues)
  }

  def commandStatus(commandId: String) = {
    val cmd = runningCommands.get(commandId)
    if (!cmd.isDefined) {
      SchedoscopeCommandStatus(commandId, "", None, Map("non-existent" -> 1))
    } else {
      val statusCounts = cmd.get.parts
        .map(f => {
          if (f.isCompleted)
            Await.result(f, 0 seconds) match {
              // FIXME: Here we have to map all possible return messages to status codes
              case ViewMaterialized(view, incomplete, changed, errors) => "materialized"
              case NoDataAvailable(view) => "no-data"
              case Failed(view) => "failed"
              case ViewStatusResponse(status, view, actor) => status
              case _ => "other"
            }
          else
            "running"
        })
        .groupBy(_.toString)
        .mapValues(_.size)

      if (statusCounts.get("running").getOrElse(0) == 0) {
        finalizeInternal(commandId, cmd.get.start, Map("submitted" -> cmd.get.parts.size) ++ statusCounts)
      } else {
        SchedoscopeCommandStatus(commandId, cmd.get.start, None, Map("submitted" -> cmd.get.parts.size) ++ statusCounts)
      }
    }
  }

  def commands(status: Option[String], filter: Option[String]) = {
    val running = runningCommands.keys.map(id => {
      commandStatus(id)
    }).toList
    (running ++ doneCommands.values)
      .groupBy(_.id)
      .map(p => p._2.head)
      .toSeq
      .sortBy(_.start.toString)
      .filter(c => if (!status.isDefined || status.get.isEmpty) true else (c.status.get(status.get).isDefined && c.status(status.get) > 0))
      .filter(c => !filter.isDefined || c.id.matches(filter.get))
      .toList
  }

  def shutdown(): Boolean = {
    actorSystem.shutdown()
    actorSystem.awaitTermination(5 seconds)
    actorSystem.isTerminated
  }
}
