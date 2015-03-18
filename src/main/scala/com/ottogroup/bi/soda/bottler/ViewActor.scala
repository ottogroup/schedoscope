package com.ottogroup.bi.soda.bottler

import java.security.PrivilegedAction

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.ottogroup.bi.soda.SettingsImpl
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

  val listeningDependant = collection.mutable.HashSet[ActorRef]()
  val waitingForDependency = collection.mutable.HashSet[View]()
  var aDependencyReturnedData = false

  // state variables
  // one of the dependencies was not available (no data)
  var incomplete = false

  // one of the dependencies or the view itself was recreated
  var changed = false

  // one of the dependencies' transformations failed
  var withErrors = false

  override def postRestart(reason: Throwable) {
    self ! MaterializeView()
  }

  // State: default
  // transitions: materialized,waiting,nodata
  def receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("receive", view)

    case MaterializeView() => materializeDependencies
  })

  // State: waiting
  // Description: Waiting for dependencies to materialize
  // transitions: waiting, transforming, materialized
  def waiting: Receive = LoggingReceive {
    case _: GetStatus => sender ! ViewStatusResponse("waiting", view)

    case MaterializeView() => listeningDependant.add(sender)

    case NoDataAvailable(dependency) => {
      log.debug("received nodata from " + dependency)
      incomplete = true
      dependencyAnswered(dependency)
    }

    case Failed(dependency) => {
      log.debug("received failed from " + dependency)
      incomplete = true
      withErrors = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, true, false, false) => {
      log.debug("incomplete,not changed from " + dependency)
      aDependencyReturnedData = true
      incomplete = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, false, false, false) => {
      log.debug("complete,not changed from " + dependency)
      aDependencyReturnedData = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, true, true, false) => {
      log.debug("incomplete,changed from " + dependency)
      aDependencyReturnedData = true
      incomplete = true
      changed = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, false, true, false) => {
      log.debug("complete, changed from " + dependency)
      aDependencyReturnedData = true
      changed = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, true, false, true) => {
      log.debug("incomplete,not changed from " + dependency)
      aDependencyReturnedData = true
      incomplete = true
      withErrors = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, false, false, true) => {
      log.debug("complete,not changed from " + dependency)
      aDependencyReturnedData = true
      withErrors = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, true, true, true) => {
      log.debug("incomplete,changed from " + dependency);
      aDependencyReturnedData = true
      incomplete = true
      changed = true
      withErrors = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, false, true, true) => {
      log.debug("complete, changed from " + dependency)
      aDependencyReturnedData = true
      changed = true
      withErrors = true
      dependencyAnswered(dependency)
    }
  }
  
  // State: transforming
  // transitions: materialized,failed,retrying
  def transforming(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse(if (0.equals(retries)) "transforming" else "retrying", view)

    case _: ActionSuccess[_] => {
      log.info("SUCCESS")

      Await.result(actionsManagerActor ? Touch(view.fullPath + "/_SUCCESS"), settings.fileActionTimeout)
      Await.result(schemaActor ? SetVersion(view), settings.schemaActionTimeout)

      unbecome()
      become(materialized)

      log.debug("STATE CHANGE:transforming->materialized")

      listeningDependant.foreach(s => { log.debug(s"sending View materialized message to ${s}"); s ! ViewMaterialized(view, incomplete, true, withErrors) })
      listeningDependant.clear
    }

    case _: ActionFailure[_] => retry(retries)

    case MaterializeView() => listeningDependant.add(sender)
  })

  // State: materialized
  // transitions: receive,materialized,transforming
  def materialized: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("materialized", view)

    case MaterializeView() => sender ! ViewMaterialized(view, incomplete, false, withErrors)

    case Invalidate() =>
      unbecome(); become(receive); log.debug("STATE CHANGE:receive")

    case NewDataAvailable(view) => if (waitingForDependency.contains(view)) reload()

  })

  // State: nodata
  // transitions: receive,transforming,nodata
  def nodata: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("nodata", view)

    case MaterializeView() => sender ! NoDataAvailable(view)

    case Invalidate() => {
      unbecome()
      become(receive)

      log.debug("STATE CHANGE:nodata->receive")
    }

    case NewDataAvailable(view) => if (waitingForDependency.contains(view)) reload()
  })

  // State: retrying
  // transitions: failed, transforming
  def retrying(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("retrying", view)

    case MaterializeView() => listeningDependant.add(sender)

    case Retry() => if (retries <= settings.retries)
      transform(retries + 1)
    else {
      unbecome()
      become(failed)

      listeningDependant.foreach(_ ! Failed(view))
      listeningDependant.clear()

      log.warning("STATE CHANGE:retrying-> failed")
    }
  })

  // State: failed
  // transitions:  waiting
  def failed: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("failed", view)

    case NewDataAvailable(view) => if (waitingForDependency.contains(view)) reload()

    case Invalidate() => {
      unbecome()
      become(receive)
      log.debug("STATE CHANGE:failed->receive")
    }

    case MaterializeView() => sender ! Failed(view)

    case _ => sender ! FatalError(view, "not recoverable")
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

  def transform(retries: Int) = {
    val partitionResult = schemaActor ? AddPartition(view)
    Await.result(partitionResult, settings.schemaActionTimeout)
    Await.result(actionsManagerActor ? Delete(view.fullPath, true), settings.fileActionTimeout)

    actionsManagerActor ! view

    unbecome()
    become(transforming(retries))

    log.debug("STATE CHANGE:transforming")
  }

  def successFlagExists(view: View): Boolean = {
    settings.userGroupInformation.doAs(new PrivilegedAction[Boolean]() {
      def run() = {
        val pathWithSuccessFlag = new Path(view.fullPath + "/_SUCCESS")

        FileSystem.get(settings.hadoopConf).exists(pathWithSuccessFlag)
      }
    })
  }

  def retry(retries: Int): akka.actor.Cancellable = {
    unbecome()
    become(retrying(retries))

    log.warning("STATE CHANGE: transforming->retrying")

    // exponential backoff
    system.scheduler.scheduleOnce(Duration.create(Math.pow(2, retries).toLong, "seconds"))(self ! Retry())
  }

  def dependencyAnswered(dependency: com.ottogroup.bi.soda.dsl.View) = {
    waitingForDependency.remove(dependency)

    log.debug(s"this actor still waits for ${waitingForDependency.size} dependencies, changed=${changed}, incomplete=${incomplete}, dependencies with data=${aDependencyReturnedData}")

    if (waitingForDependency.isEmpty) {
      if (aDependencyReturnedData) {
        if (!changed) {
          val versionInfo = Await.result(schemaActor ? CheckVersion(view), settings.schemaActionTimeout)
          log.debug(versionInfo.toString)
          versionInfo match {
            case SchemaActionFailure() => {
              changed = true
              log.warning("got error from versioncheck, assuming changed data")
            }

            case VersionOk(v) =>

            case VersionMismatch(v, version) => changed = true; log.debug("version mismatch,assuming changed workflow")
          }
        }

        if (changed)
          transform(0)
        else {
          listeningDependant.foreach(s => s ! ViewMaterialized(view, incomplete, false, withErrors))
          listeningDependant.clear
          aDependencyReturnedData = false

          unbecome
          become(materialized)

          log.debug("STATE CHANGE:waiting->materialized")
        }
      } else {
        listeningDependant.foreach(s => { log.debug(s"sending NoDataAvailable to ${s}"); s ! NoDataAvailable(view) })
        listeningDependant.clear
        aDependencyReturnedData = false

        unbecome
        become(receive)

        log.debug("STATE CHANGE:waiting->receive")
      }
    }
  }

  def materializeDependencies: Unit = {
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
      view.dependencies.foreach { dependency =>
        {
          log.debug("querying dependency " + dependency)

          val actor = Await.result((viewManagerActor ? dependency).mapTo[ActorRef], timeout.duration)
          waitingForDependency.add(dependency)

          actor ! MaterializeView()
        }
      }

      become(waiting)

      log.debug("STATE CHANGE:receive -> waiting")
    }

    listeningDependant.add(sender)
  }
}

object ViewActor {
  def props(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewActor], view, settings, viewManagerActor, actionsManagerActor, schemaActor)
}
