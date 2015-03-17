package com.ottogroup.bi.soda.bottler.api

import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import org.codehaus.jackson.map.ObjectMapper
import org.joda.time.format.DateTimeFormat
import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.ottogroup.bi.soda.bottler.ActionsManagerActor
import com.ottogroup.bi.soda.bottler.Deploy
import com.ottogroup.bi.soda.bottler.Failed
import com.ottogroup.bi.soda.bottler.GetStatus
import com.ottogroup.bi.soda.bottler.InternalError
import com.ottogroup.bi.soda.bottler.KillAction
import com.ottogroup.bi.soda.bottler.NewDataAvailable
import com.ottogroup.bi.soda.bottler.NoDataAvailable
import com.ottogroup.bi.soda.bottler.SchemaActor
import com.ottogroup.bi.soda.bottler.ViewManagerActor
import com.ottogroup.bi.soda.bottler.ViewMaterialized
import com.ottogroup.bi.soda.bottler.ViewStatusResponse
import com.ottogroup.bi.soda.bottler.ViewStatusRetriever
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.ParsedViewAugmentor
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.viewNames
import akka.actor.ActorRef
import com.ottogroup.bi.soda.bottler.SodaRootActor.actionsManagerActor
import com.ottogroup.bi.soda.bottler.SodaRootActor.settings
import com.ottogroup.bi.soda.bottler.SodaRootActor.viewManagerActor
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.bottler.ViewStatusListResponse
import com.ottogroup.bi.soda.bottler.ActionStatusListResponse

class SodaSystem extends SodaInterface {
  /*
   *  setup soda actor system
   */
  implicit val ec = ExecutionContext.global
  implicit val timeout = Timeout(3 days) // needed for `?` below
  val viewAugmentor = Class.forName(settings.parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]   

  /*
   * job lists
   */
  val runningCommands = collection.mutable.HashMap[String, SodaCommand]()
  val doneComands = collection.mutable.HashMap[String, List[_]]()

  /*
   * helper methods
   */
  private def getViewActors(viewUrlPath: String) = {
    val views = View.viewsFromUrl(settings.env, viewUrlPath, viewAugmentor)
    val viewActorRefFutures = views.map { v => (viewManagerActor ? v).mapTo[ActorRef] }
    Await.result(Future sequence viewActorRefFutures, 60 seconds)
  }

  private def submitCommandInternal(actors: List[ActorRef], command: Any, args: String*) = {
    val fut = actors.map(actor => actor ? command)
    val start = new LocalDateTime()
    register(SodaCommandStatus(s"${command.toString}-${args.mkString}-${start}", start, Map("submitted" -> fut.size)), fut)
  }

  private def register(sd: SodaCommandStatus, fut: List[Future[_]]) = {
    runningCommands.put(sd.id, SodaCommand(sd.id, sd.started, fut))
    sd
  }
  
  /*
   * soda API
   */
  def materialize(viewUrlPath: String) = {
    val viewActors = getViewActors(viewUrlPath)
    submitCommandInternal(viewActors, "materialize", viewUrlPath)
  }

  def commandStatus(commandId: String) = {
    val cmd = runningCommands.get(commandId)
    if (!cmd.isDefined) {
      SodaCommandStatus(commandId, new LocalDateTime(), Map("non-existent" -> 1))
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
      SodaCommandStatus(commandId, cmd.get.started, Map("submitted" -> cmd.size) ++ statusCounts)
    }
  }

  def commands(status: String) = {
    runningCommands.keys.map( id => {
      commandStatus(id)
    }).toList     
  }

  def views(viewUrlPath: String, status: String) = {
    val result = Await.result((viewManagerActor ? GetStatus()).mapTo[ViewStatusListResponse], 1 minute)
    val views = result.viewStatusList
    val overview = views.groupBy(_.status).mapValues(_.size)
    ViewStatusList(overview, views)
  }

  def invalidate(viewUrlPath: String) = { null }

  def newdata(viewUrlPath: String) = { null }

  def actions(status: String) = {
    val result = Await.result((actionsManagerActor ? GetStatus()).mapTo[ActionStatusListResponse], 1 minute)
    val actions = result.actionStatusList
    val queues = result.actionQueueStatus
    val overview = Map(
        "running" -> actions.filter{ _.driverRunStatus.isInstanceOf[DriverRunOngoing[_]] }.size,
        "idle" -> actions.filter { _.driverRunHandle == null }.size,
        "queued" -> queues.foldLeft(0)((s, el) => s + el._2.size)
        )
    ActionStatusList(overview, queues, actions)
  }

  def dependencies(viewUrlPath: String, recursive: Boolean) = { null }  
  
}


trait SodaInterface {

  def materialize(viewUrlPath: String): SodaCommandStatus

  def commandStatus(commandId: String): SodaCommandStatus

  def commands(status: String): List[SodaCommandStatus]

  def views(viewUrlPath: String, status: String): ViewStatusList

  def invalidate(viewUrlPath: String): SodaCommandStatus

  def newdata(viewUrlPath: String): SodaCommandStatus

  def actions(status: String): ActionStatusList

  def dependencies(viewUrlPath: String, recursive: Boolean): DependencyGraph

}