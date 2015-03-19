package com.ottogroup.bi.soda.bottler.api

import scala.Option.option2Iterable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import com.ottogroup.bi.soda.bottler.ActionStatusListResponse
import com.ottogroup.bi.soda.bottler.Failed
import com.ottogroup.bi.soda.bottler.GetStatus
import com.ottogroup.bi.soda.bottler.MaterializeView
import com.ottogroup.bi.soda.bottler.NoDataAvailable
import com.ottogroup.bi.soda.bottler.SodaRootActor
import com.ottogroup.bi.soda.bottler.ViewMaterialized
import com.ottogroup.bi.soda.bottler.ViewStatusListResponse
import com.ottogroup.bi.soda.dsl.Named
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.ParsedViewAugmentor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.util.Timeout
import com.ottogroup.bi.soda.bottler.Invalidate
import com.ottogroup.bi.soda.bottler.Deploy
import com.ottogroup.bi.soda.queryActorWithMessages
import com.ottogroup.bi.soda.queryActor
import akka.pattern.Patterns
import com.ottogroup.bi.soda.bottler.ViewStatusListResponse
import com.ottogroup.bi.soda.bottler.ActionStatusListResponse

class SodaSystem extends SodaInterface {
  /*
   *  setup soda actor system
   */
  val settings = SodaRootActor.settings
  implicit val ec = ExecutionContext.global

  val viewAugmentor = Class.forName(settings.parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]
  val viewManagerActor = SodaRootActor.viewManagerActor
  val actionsManagerActor = SodaRootActor.actionsManagerActor
  val schemaActor = SodaRootActor.schemaActor

  /*
   * deploy transformation resources FIXME: we don't check for success here...
   */
  actionsManagerActor ! Deploy()

  /*
   * job lists
   */
  val runningCommands = collection.mutable.HashMap[String, SodaCommand]()
  val doneCommands = collection.mutable.HashMap[String, SodaCommandStatus]()

  /*
   * helper methods
   */

  private def getViewActors(viewUrlPath: String) = {
    val views = View.viewsFromUrl(settings.env, viewUrlPath, viewAugmentor)
    queryActorWithMessages(viewManagerActor, views, settings.viewManagerResponseTimeout).asInstanceOf[List[ActorRef]]
  }

  private def commandId(command: Any, args: Seq[String], start: Option[LocalDateTime] = None) = {
    val format = DateTimeFormat.forPattern("YYYYMMddHHmmss");
    val c = command match {
      case s: String => s
      case c: Any => Named.formatName(c.getClass.getSimpleName)
    }
    val a = if (args.size == 0) "_" else args.mkString(":")
    if (start.isDefined) {
      val s = format.print(start.get)
      s"${c}::${a}::${s}"
    } else {
      s"${c}::${a}"
    }
  }

  private def submitCommandInternal(actors: List[ActorRef], command: Any, args: String*) = {
    val currentCommandId = runningCommandId(command.toString, args: _*)
    if (currentCommandId.isDefined) {
      commandStatus(currentCommandId.get)
    } else {
      val jobFutures = actors.map { actor => Patterns.ask(actor, command, Timeout(settings.completitionTimeout)) }
      val start = new LocalDateTime()
      val id = commandId(command, args, Some(start))
      runningCommands.put(id, SodaCommand(id, start, jobFutures))
      SodaCommandStatus(id, start, null, Map("submitted" -> jobFutures.size))
    }
  }

  private def finalizeInternal(commandId: String, start: LocalDateTime, status: Map[String, Int]) = {
    runningCommands.remove(commandId)
    val scs = SodaCommandStatus(commandId, start, new LocalDateTime(), status)
    doneCommands.put(commandId, scs)
    scs
  }

  private def runningCommandId(command: String, args: String*): Option[String] = {
    val cidPrefix = commandId(command, args)
    runningCommands.keys
      .foreach(k =>
        if (k.startsWith(cidPrefix))
          return Some(k))
    None
  }

  /*
   * soda API
   */
  def materialize(viewUrlPath: String) = {
    val viewActors = getViewActors(viewUrlPath)
    submitCommandInternal(viewActors, MaterializeView(), viewUrlPath)
  }

  def invalidate(viewUrlPath: String) = { // FIXME: incomplete
    val viewActors = getViewActors(viewUrlPath)
    submitCommandInternal(viewActors, Invalidate(), viewUrlPath)
  }

  def newdata(viewUrlPath: String) = { // FIXME: incomplete
    val viewActors = getViewActors(viewUrlPath)
    submitCommandInternal(viewActors, "newdata", viewUrlPath)
  }

  def views(viewUrlPath: Option[String], status: Option[String], withDependencies: Boolean = false) = {
    println("Fetching views ...")
    val result: ViewStatusListResponse = queryActor(viewManagerActor, GetStatus(), settings.statusListAggregationTimeout)
    val views = result.viewStatusList
      .map(v => ViewStatus(v.view.urlPath, v.status, None, if (!withDependencies) None else Some(v.view.dependencies.map(d => d.urlPath).toList)))
      .filter(v => status.getOrElse(v.status).equals(v.status))
    val overview = views.groupBy(_.status).mapValues(_.size)
    ViewStatusList(overview, views)
  }

  def actions(status: Option[String]) = {
    println("Fetching actions ...")
    val result: ActionStatusListResponse = queryActor(actionsManagerActor, GetStatus(), settings.statusListAggregationTimeout)
    val actions = result.actionStatusList
      .map(a => SodaJsonProtocol.parseActionStatus(a))
      .filter(a => status.getOrElse(a.status).equals(a.status))
    val queues = result.actionQueueStatus
      .filter(el => status.getOrElse("queued").equals("queued"))
    val running = actions
      .groupBy(_.status)
      .map(el => (el._1, el._2.size))
    val queued = Map("queued" -> queues.foldLeft(0)((s, el) => s + el._2.size))
    ActionStatusList(running ++ queued, queues, actions)
  }

  def commandStatus(commandId: String) = {
    val cmd = runningCommands.get(commandId)
    if (!cmd.isDefined) {
      SodaCommandStatus(commandId, null, null, Map("non-existent" -> 1))
    } else {
      val statusCounts = cmd.get.parts
        .map(f => {
          if (f.isCompleted)
            Await.result(f, 0 seconds) match {
              // FIXME: Here we have to map all possible return messages to status codes
              case ViewMaterialized(view, incomplete, changed, errors) => "materialized"
              case NoDataAvailable(view) => "no-data"
              case Failed(view) => "failed"
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
        SodaCommandStatus(commandId, cmd.get.start, null, Map("submitted" -> cmd.get.parts.size) ++ statusCounts)
      }
    }
  }

  def commands(status: Option[String]) = {
    val running = runningCommands.keys.map(id => {
      commandStatus(id)
    }).toList
    (running ++ doneCommands.values)
      .groupBy(_.id)
      .map(p => p._2.head)
      .toSeq
      .sortBy(_.start.toString)
      .filter(c => if (!status.isDefined) true else (c.status.get(status.get).isDefined && c.status(status.get) > 0))
      .toList
  }

}
