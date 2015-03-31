package com.ottogroup.bi.soda.bottler

import java.lang.Math.max
import java.security.PrivilegedAction
import java.util.Date

import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.ottogroup.bi.soda.SettingsImpl
import com.ottogroup.bi.soda.dsl.NoOp
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.FilesystemTransformation
import com.ottogroup.bi.soda.dsl.transformations.Touch

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive

class ViewActor(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._

  val log = Logging(system, this)

  val listenersWaitingForMaterialize = collection.mutable.HashSet[ActorRef]()
  val dependenciesMaterializing = collection.mutable.HashSet[View]()
  var oneDependencyReturnedData = false

  // state variables
  // timestamp of last transformation
  var lastTransformationTimestamp = 0l

  // has version mismatch checking already occurred with the current transformation
  var versionMismatchCheckedAlready = false

  // one of the dependencies was not available (no data)
  var incomplete = false

  // maximum transformation timestamp of dependencies
  var dependenciesFreshness = 0l

  // one of the dependencies' transformations failed
  var withErrors = false

  override def preStart {
    logStateInfo("receive")
  }

  override def postRestart(reason: Throwable) {
    self ! MaterializeView()
  }

  // State: default
  // transitions: defaultForViewWithoutDependencies, defaultForViewWithDependencies
  def receive: Receive = LoggingReceive({
    case MaterializeView() => if (view.dependencies.isEmpty) {
      listenersWaitingForMaterialize.add(sender)
      toTransformOrMaterialize(0)
    } else {
      toWaiting()
    }

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.isEmpty) {
      if (view == viewWithNewData) toDefaultAndReload(false)
    } else {
      if (view.dependencies.contains(viewWithNewData)) toDefaultAndReload()
    }
  })

  // State: view actor waiting for dependencies to materialize
  // transitions: transforming, materialized, default
  def waiting: Receive = LoggingReceive {

    case MaterializeView() => listenersWaitingForMaterialize.add(sender)

    case NoDataAvailable(dependency) => {
      log.debug("Nodata from " + dependency)
      incomplete = true
      dependencyAnswered(dependency)
    }

    case Failed(dependency) => {
      log.debug("Failed from " + dependency)
      incomplete = true
      withErrors = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, dependencyIncomplete, dependencyTransformationTimestamp, dependencyWithErrors) => {
      log.debug(s"View materialized from ${dependency}: incomplete=${dependencyIncomplete} transformationTimestamp=${dependencyTransformationTimestamp} withErrors=${dependencyWithErrors}")
      oneDependencyReturnedData = true
      incomplete |= dependencyIncomplete
      withErrors |= dependencyWithErrors
      dependenciesFreshness = max(dependenciesFreshness, dependencyTransformationTimestamp)
      dependencyAnswered(dependency)
    }

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData)) self ! NewDataAvailable(viewWithNewData)
  }

  // State: transforming, view actor in process of applying transformation
  // transitions: materialized,retrying
  def transforming(retries: Int): Receive = LoggingReceive({
    //case GetStatus() => sender ! ViewStatusResponse(if (0.equals(retries)) "transforming" else "retrying", view)

    case _: ActionSuccess[_] => {
      log.info("SUCCESS")

      setVersion(view)
      touchSuccessFlag(view)
      logTransformationTimestamp(view)

      toMaterialize()
    }

    case _: ActionFailure[_] => toRetrying(retries)

    case MaterializeView() => listenersWaitingForMaterialize.add(sender)

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData) || (view.dependencies.isEmpty && viewWithNewData == view)) self ! NewDataAvailable(viewWithNewData)
  })

  // State: retrying
  // transitions: failed, transforming
  def retrying(retries: Int): Receive = LoggingReceive({

    case MaterializeView() => listenersWaitingForMaterialize.add(sender)

    case Retry() => if (retries <= settings.retries)
      toTransformOrMaterialize(retries + 1)
    else {
      logStateInfo("failed")
      unbecomeBecome(failed)

      listenersWaitingForMaterialize.foreach(_ ! Failed(view))
      listenersWaitingForMaterialize.clear()
    }

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData) || (view.dependencies.isEmpty && viewWithNewData == view)) self ! NewDataAvailable(viewWithNewData)
  })

  // State: materialized, view has been computed and materialized
  // transitions: default,transforming
  def materialized: Receive = LoggingReceive({

    case MaterializeView() => {
      if (view.dependencies.isEmpty) {
        sender ! ViewMaterialized(view, incomplete, getTransformationTimestamp(view), withErrors)
      } else {
        toWaiting()
      }
    }

    case Invalidate() => {
      sender ! ViewStatusResponse("invalidated", view)
      toDefault(true)
    }

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData))
      toDefaultAndReload()
    else if (view.dependencies.isEmpty && view == viewWithNewData)
      toDefaultAndReload(false)
  })

  // State: failed, view actor failed to materialize
  // transitions:  default, transforming
  def failed: Receive = LoggingReceive({
    case NewDataAvailable(viewWithNewData) =>
      if (view.dependencies.contains(viewWithNewData)) toDefaultAndReload()
      else if (view.dependencies.isEmpty && view == viewWithNewData) toDefaultAndReload(false)

    case Invalidate() => {
      sender ! ViewStatusResponse("invalidated", view)
      toDefault(true)
    }

    case MaterializeView() => sender ! Failed(view)
  })

  def dependencyAnswered(dependency: com.ottogroup.bi.soda.dsl.View) {
    dependenciesMaterializing.remove(dependency)

    if (!dependenciesMaterializing.isEmpty) {
      log.debug(s"This actor is still waiting for ${dependenciesMaterializing.size} dependencies, dependencyFreshness=${dependenciesFreshness}, incomplete=${incomplete}, dependencies with data=${oneDependencyReturnedData}")
      return
    }

    if (oneDependencyReturnedData) {
      if ((getTransformationTimestamp(view) <= dependenciesFreshness) || hasVersionMismatch(view))
        toTransformOrMaterialize(0)
      else {
        toMaterialize()
      }
    } else {
      setVersion(view)

      listenersWaitingForMaterialize.foreach(s => { log.debug(s"sending NoDataAvailable to ${s}"); s ! NoDataAvailable(view) })
      listenersWaitingForMaterialize.clear

      toDefault()
    }
  }

  def toDefault(invalidate: Boolean = false) {
    lastTransformationTimestamp = if (invalidate) -1l else 0l
    dependenciesFreshness = 0l
    withErrors = false
    incomplete = false

    logStateInfo(if (invalidate) "invalidated" else "receive")

    unbecomeBecome(receive)
  }

  def toWaiting() {
    withErrors = false
    incomplete = false
    dependenciesFreshness = 0l

    listenersWaitingForMaterialize.add(sender)

    logStateInfo("waiting")

    view.dependencies.foreach { d =>
      {
        dependenciesMaterializing.add(d)

        log.debug("sending materialize to dependency " + d)

        getViewActor(d) ! MaterializeView()
      }
    }

    unbecomeBecome(waiting)

  }

  def toMaterialize() {
    logStateInfo("materialized")

    listenersWaitingForMaterialize.foreach(s => s ! ViewMaterialized(view, incomplete, getTransformationTimestamp(view), withErrors))
    listenersWaitingForMaterialize.clear

    unbecomeBecome(materialized)

    oneDependencyReturnedData = false
    dependenciesFreshness = 0l
  }

  def toTransformOrMaterialize(retries: Int) {
    view.transformation() match {
      case NoOp() => {
        setVersion(view)

        if (successFlagExists(view)) {
          log.debug("no dependencies for " + view + ", success flag exists, and no transformation specified")
          getOrLogTransformationTimestamp(view)

          toMaterialize()
        } else {
          log.debug("no data and no dependencies for " + view)

          listenersWaitingForMaterialize.foreach(s => s ! NoDataAvailable(view))
          listenersWaitingForMaterialize.clear

          toDefault()
        }
      }

      case _: FilesystemTransformation => {
        if (getTransformationTimestamp(view) > 0l) {
          toMaterialize()
        } else {
          actionsManagerActor ! view

          logStateInfo("transforming")

          unbecomeBecome(transforming(retries))
        }
      }

      case _ => {
        actionsManagerActor ! view

        logStateInfo("transforming")

        unbecomeBecome(transforming(retries))
      }
    }

  }

  def toRetrying(retries: Int): akka.actor.Cancellable = {
    logStateInfo("retrying")

    unbecomeBecome(retrying(retries))

    // exponential backoff
    system.scheduler.scheduleOnce(Duration.create(Math.pow(2, retries).toLong, "seconds"))(self ! Retry())
  }

  def toDefaultAndReload(withPropagation: Boolean = true) {
    toDefault()

    self ! MaterializeView()
    if (withPropagation)
      viewManagerActor ! NewDataAvailable(view)
  }

  def successFlagExists(view: View): Boolean = {
    settings.userGroupInformation.doAs(new PrivilegedAction[Boolean]() {
      def run() = {
        val pathWithSuccessFlag = new Path(view.fullPath + "/_SUCCESS")

        FileSystem.get(settings.hadoopConf).exists(pathWithSuccessFlag)
      }
    })
  }

  def touchSuccessFlag(view: View) {
    queryActor(actionsManagerActor, Touch(view.fullPath + "/_SUCCESS"), settings.filesystemTimeout)
  }

  def hasVersionMismatch(view: View) = {
    if (!versionMismatchCheckedAlready) {
      versionMismatchCheckedAlready = true

      val versionInfo: AnyRef = queryActor(schemaActor, CheckViewVersion(view), settings.schemaTimeout)

      versionInfo match {
        case SchemaActionFailure() => {
          log.warning("got error from versioncheck, assuming version mismatch")
          true
        }

        case ViewVersionMismatch(v, version) => {
          log.debug("version mismatch")
          true
        }

        case ViewVersionOk(v) => false
      }
    }

    false
  }

  def getViewActor(view: View) = {
    val viewActor = ViewManagerActor.actorForView(view)
    if (!viewActor.isTerminated) {
      viewActor
    } else {
      queryActor(viewManagerActor, view, settings.viewManagerResponseTimeout)
    }
  }

  def getTransformationTimestamp(view: View) = {
    if (lastTransformationTimestamp == 0l) {
      lastTransformationTimestamp = queryActor(schemaActor, GetTransformationTimestamp(view), settings.schemaTimeout).asInstanceOf[AnyRef] match {
        case TransformationTimestamp(_, ts) => ts
        case _ => 0l
      }
    }
    lastTransformationTimestamp
  }

  def logTransformationTimestamp(view: View) = {
    val timestamp = new Date().getTime()
    schemaActor ! LogTransformationTimestamp(view, timestamp)
    timestamp
  }

  def getOrLogTransformationTimestamp(view: View) = {
    val ts = getTransformationTimestamp(view)
    if (ts <= 0l)
      logTransformationTimestamp(view)
    else ts
  }

  def setVersion(view: View) {
    queryActor(schemaActor, SetViewVersion(view), settings.schemaTimeout)
  }

  private def unbecomeBecome(behaviour: Actor.Receive) {
    unbecome()
    become(behaviour)
  }

  def logStateInfo(stateName: String) {
    viewManagerActor ! ViewStatusResponse(stateName, view)
    log.info(s"VIEWACTOR STATE CHANGE ===> ${stateName.toUpperCase()}: lastTransformationTimestamp=${lastTransformationTimestamp} dependenciesFreshness=${dependenciesFreshness} incomplete=${incomplete} withErrors=${withErrors}")
  }
}

object ViewActor {
  def props(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewActor], view, settings, viewManagerActor, actionsManagerActor, schemaActor)
}
