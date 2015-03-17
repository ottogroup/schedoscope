package com.ottogroup.bi.soda.bottler

import java.security.PrivilegedAction

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import com.ottogroup.bi.soda.dsl.NoOp
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Delete
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Touch

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout

class ViewActor(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._
  val log = Logging(system, this)
  implicit val timeout = new Timeout(settings.dependencyTimout)

  val listeners = collection.mutable.HashSet[ActorRef]()
  val dependencies = collection.mutable.HashSet[View]()
 

  // state variables
  // one of the dependencies was not available (no data)
  var incomplete = false

  // one of the dependencies or the view itself was recreated
  var changed = false

  // one of the dependencies' transformations failed
  var withErrors = false
  
  var availableDependencies = 0

  override def postRestart(reason: Throwable) {
    self ! "materialize"
  }

  def receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("receive", view)
    case "materialize" => materializeDependencies
  })

  def transform(retries: Int) = {
    val partitionResult = schemaActor ? AddPartition(view)
    Await.result(partitionResult, settings.schemaActionTimeout)
    Await.result(actionsManagerActor ? Delete(view.fullPath, true), settings.fileActionTimeout)

    actionsManagerActor ! view

    unbecome()
    become(transforming(retries))

    log.debug("STATE CHANGE:transforming")
  }
  
  // State: transforming
  // transitions: materialized,failed,transforming
  def transforming(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse(if (0.equals(retries)) "transforming" else "retrying", view)

    case _: ActionSuccess[_] => {
      log.info("SUCCESS")

      Await.result(actionsManagerActor ? Touch(view.fullPath + "/_SUCCESS"), settings.fileActionTimeout)
      Await.result(schemaActor ? SetVersion(view), settings.schemaActionTimeout)

      unbecome()
      become(materialized)

      log.debug("STATE CHANGE:transforming->materialized")

      listeners.foreach(s => { log.debug(s"sending VMI to ${s}"); s ! ViewMaterialized(view, incomplete, true, withErrors) })
      listeners.clear
    }

    case _: ActionFailure[_] => retry(retries)

    case "materialize" => listeners.add(sender)
  })

  def reload() = {
    unbecome()
    become(waiting)

    log.debug("STATE CHANGE:waiting")

    Await.result(actionsManagerActor ? Delete(view.fullPath + "/_SUCCESS", false), settings.fileActionTimeout)
    transform(1)

    // tell everyone that new data is avaiable
    system.actorSelection("/user/views/*") ! NewDataAvailable(view)
  }

  // State: materialized
  // transitions: receive,materialized,transforming
  def materialized: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("materialized", view)

    case "materialize" => sender ! ViewMaterialized(view, incomplete, false, withErrors)
    case "invalidate" =>
      unbecome(); become(receive); log.debug("STATE CHANGE:receive")

    case NewDataAvailable(view) => if (dependencies.contains(view)) reload()

  })

  // State: nodata
  // transitions; receive,transforming,nodata
  def nodata: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("nodata", view)

    case "materialize" => sender ! NoDataAvailable(view)
    case "invalidate" => {
      unbecome()
      become(receive)

      log.debug("STATE CHANGE:nodata->receive")
    }

    case NewDataAvailable(view) => if (dependencies.contains(view)) reload()
  })

  def retrying(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("retrying", view)

    case "materialize" => listeners.add(sender)

    case "retry" => if (retries <= settings.retries)
      transform(retries + 1)
    else {
      unbecome()
      become(failed)

      listeners.foreach(_ ! Failed(view))
      listeners.clear()

      log.warning("STATE CHANGE:retrying-> failed")
    }
  })

  def failed: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("failed", view)
    case NewDataAvailable(view) => if (dependencies.contains(view)) reload()
    case "invalidate" =>
      unbecome(); become(receive); log.debug("STATE CHANGE:failed->receive")
    case "materialize" => sender ! Failed(view)
    case _ => sender ! FatalError(view, "not recoverable")
  })

  // State: waiting
  // Description: Waiting for dependencies to materialize
  // transitions: waiting, transforming, materialized
  def waiting: Receive = LoggingReceive {
    case _: GetStatus => sender ! ViewStatusResponse("waiting", view)

    case "materialize" => listeners.add(sender)

    case NoDataAvailable(dependency) => {
      log.debug("received nodata from " + dependency);
      incomplete = true; availableDependencies -= 1;
      waitingDependencyIsDone(dependency)
    }
    case Failed(dependency) => {
      log.debug("received failed from " + dependency);
      incomplete = true; withErrors = true; availableDependencies -= 1;
      waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, true, false, false) => {
      log.debug("incomplete,not changed from " + dependency);
      incomplete = true; waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, false, false, false) => {
      log.debug("complete,not changed from " + dependency);
      waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, true, true, false) => {
      log.debug("incomplete,changed from " + dependency);
      incomplete = true; changed = true;
      waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, false, true, false) => {
      log.debug("complete, changed from " + dependency);
      changed = true; waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, true, false, true) => {
      log.debug("incomplete,not changed from " + dependency);
      incomplete = true;
      withErrors = true;
      waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, false, false, true) => {
      log.debug("complete,not changed from " + dependency);
      withErrors = true;
      waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, true, true, true) => {
      log.debug("incomplete,changed from " + dependency);
      incomplete = true; changed = true; withErrors = true; waitingDependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, false, true, true) => {
      log.debug("complete, changed from " + dependency);
      changed = true; withErrors = true; waitingDependencyIsDone(dependency)
    }
  }

  def successFlagExists(view: View): Boolean = {
    settings.userGroupInformation.doAs(new PrivilegedAction[Boolean]() {
      def run() = {
        val pathWithSuccessFlag = new Path(view.fullPath + "/_SUCCESS")

        FileSystem.get(settings.hadoopConf).exists(pathWithSuccessFlag)
      }
    })
  }

  private def retry(retries: Int): akka.actor.Cancellable = {
    unbecome()
    become(retrying(retries))

    log.warning("STATE CHANGE: transforming->retrying")

    // exponential backoff
    system.scheduler.scheduleOnce(Duration.create(Math.pow(2, retries).toLong, "seconds"))(self ! "retry")
  }

  private def waitingDependencyIsDone(dependency: com.ottogroup.bi.soda.dsl.View) = {
    dependencies.remove(dependency)
    log.debug(s"this actor still waits for ${dependencies.size} dependencies, changed=${changed}, incomplete=${incomplete}, availableDependencies=${availableDependencies}")
    if (dependencies.isEmpty) {
      // there where dependencies that did not return nodata
      if (availableDependencies > 0) {

        if (!changed) {
          val versionInfo = Await.result(schemaActor ? CheckVersion(view), settings.schemaActionTimeout)
          log.debug(versionInfo.toString)
          versionInfo match {
            case Error =>
              changed = true; log.warning("got error from versioncheck, assuming changed data")
            case VersionOk(v) =>
            case VersionMismatch(v, version) => changed = true; log.debug("version mismatch,assuming changed workflow")
          }
        }

        if (changed)
          transform(0)
        else {
          listeners.foreach(s => s ! ViewMaterialized(view, incomplete, false, withErrors))
          listeners.clear

          unbecome
          become(materialized)

          log.debug("STATE CHANGE:waiting->materialized")
        }
      } else {
        listeners.foreach(s => { log.debug(s"sending NoDataAvailable to ${s}"); s ! NoDataAvailable(view) })
        listeners.clear

        unbecome
        become(receive)

        log.debug("STATE CHANGE:waiting->receive")

      }
    }
  }

  private def materializeDependencies: Unit = {
    log.debug(view + " has dependencies " + view.dependencies.mkString(", "))
    if (view.dependencies.isEmpty) {
      if (successFlagExists(view) && view.transformation() == NoOp()) {
        log.debug("no dependencies for " + view + ", success flag exists, and no transformation specified")

        sender ! ViewMaterialized(view, false, false, withErrors)

        become(materialized)

        log.debug("STATE CHANGE:receive->materialized")
      } else if (view.transformation() != NoOp()) {
        log.debug("no dependencies for " + view + " and transformation specified")

        transform(0)
      } else {
        log.debug("no data and no dependencies for " + view)

        sender ! NoDataAvailable(view)
        become(nodata)

        log.debug("STATE CHANGE:receive -> nodata")
      }
    } else {
      view.dependencies.foreach { dependendView =>
        {
          log.debug("querying dependency " + dependendView)

          val actor = Await.result((viewManagerActor ? dependendView).mapTo[ActorRef], timeout.duration)
          dependencies.add(dependendView)
          availableDependencies += 1

          actor ! "materialize"
        }
      }

      become(waiting)

      log.debug("STATE CHANGE:receive -> waiting")
    }

    listeners.add(sender)
  }
}

object ViewActor {
  def props(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewActor], view, settings, viewManagerActor, actionsManagerActor, schemaActor)
}
