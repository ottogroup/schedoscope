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
package org.schedoscope.scheduler.actors

import java.lang.Math.max
import java.security.PrivilegedAction
import java.util.Date
import akka.actor.ActorSelection.toScala
import akka.actor.{ Actor, ActorRef, Props, actorRef2Scala }
import akka.event.{ Logging, LoggingReceive }
import org.apache.hadoop.fs.{ Path, PathFilter }
import org.schedoscope.{ AskPattern, SchedoscopeSettings }
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.{ FilesystemTransformation, NoOp, Touch }
import org.schedoscope.scheduler.driver.FileSystemDriver.defaultFileSystem
import org.schedoscope.scheduler.messages._
import scala.concurrent.duration.Duration
import org.schedoscope.dsl.transformations.FilesystemTransformation

class ViewActor(view: View, settings: SchedoscopeSettings, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef, var versionChecksum: String = null, var lastTransformationTimestamp: Long = 0l) extends Actor {

  import AskPattern._
  import MaterializeViewMode._
  import context._

  val log = Logging(system, this)

  val listenersWaitingForMaterialize = collection.mutable.HashSet[ActorRef]()
  val dependenciesMaterializing = collection.mutable.HashSet[View]()
  var knownDependencies = view.dependencies.toSet

  var oneDependencyReturnedData = false

  // state variables
  // timestamp of last transformation

  // one of the dependencies was not available (no data)
  var incomplete = false

  // maximum transformation timestamp of dependencies
  var dependenciesFreshness = 0l

  // one of the dependencies' transformations failed
  var withErrors = false

  override def preStart {
    logStateInfo("receive", false)
  }

  // State: default
  // transitions: defaultForViewWithoutDependencies, defaultForViewWithDependencies
  def receive: Receive = LoggingReceive({
    case MaterializeView(mode) => {
      if (RESET_TRANSFORMATION_CHECKSUMS == mode) {
        val before = versionChecksum
        setVersion(view)
        log.info(s"VIEWACTOR CHECKSUM RESET ===> before=${before} , after=${versionChecksum}")
      }

      if (view.dependencies.isEmpty) {
        listenersWaitingForMaterialize.add(sender)
        toTransformingOrMaterialized(0, mode)
      } else {
        toWaiting(mode)
      }
    }
  })

  // State: view actor waiting for dependencies to materialize
  // transitions: transforming, materialized, default
  def waiting(materializationMode: MaterializeViewMode): Receive = LoggingReceive {

    case MaterializeView(mode) => listenersWaitingForMaterialize.add(sender)

    case NoDataAvailable(dependency) => {
      log.debug("Nodata from " + dependency)
      incomplete = true
      aDependencyAnswered(dependency, materializationMode)
    }

    case Failed(dependency) => {
      log.debug("Failed from " + dependency)
      incomplete = true
      withErrors = true
      aDependencyAnswered(dependency, materializationMode)
    }

    case ViewMaterialized(dependency, dependencyIncomplete, dependencyTransformationTimestamp, dependencyWithErrors) => {
      log.debug(s"View materialized from ${dependency}: incomplete=${dependencyIncomplete} transformationTimestamp=${dependencyTransformationTimestamp} withErrors=${dependencyWithErrors}")
      oneDependencyReturnedData = true
      incomplete |= dependencyIncomplete
      withErrors |= dependencyWithErrors
      dependenciesFreshness = max(dependenciesFreshness, dependencyTransformationTimestamp)
      aDependencyAnswered(dependency, materializationMode)
    }
  }

  // State: transforming, view actor in process of applying transformation
  // transitions: materialized,retrying
  def transforming(retries: Int, materializationMode: MaterializeViewMode): Receive = LoggingReceive({

    case _: TransformationSuccess[_] => {
      log.info("SUCCESS")

      setVersion(view)
      if (view.transformation().isInstanceOf[FilesystemTransformation]) {
        if (viewDirectorySize > 0l) {
          touchSuccessFlag(view)
          logTransformationTimestamp(view)
          toMaterialized()
        } else {
          log.debug("filesystem transformation generated no data for" + view)

          listenersWaitingForMaterialize.foreach(s => s ! NoDataAvailable(view))
          listenersWaitingForMaterialize.clear

          toDefault(false, "nodata")
        }
      } else {
        touchSuccessFlag(view)
        logTransformationTimestamp(view)
        toMaterialized()
      }
    }

    case _: TransformationFailure[_] => toRetrying(retries, materializationMode)

    case MaterializeView(mode)       => listenersWaitingForMaterialize.add(sender)
  })

  // State: retrying
  // transitions: failed, transforming
  def retrying(retries: Int, materializationMode: MaterializeViewMode): Receive = LoggingReceive({

    case MaterializeView(mode) => listenersWaitingForMaterialize.add(sender)

    case Retry() => if (retries <= settings.retries)
      toTransformingOrMaterialized(retries + 1, materializationMode)
    else {
      logStateInfo("failed")
      become(failed)

      listenersWaitingForMaterialize.foreach(_ ! Failed(view))
      listenersWaitingForMaterialize.clear()
    }
  })

  // State: materialized, view has been computed and materialized
  // transitions: default,transforming
  def materialized: Receive = LoggingReceive({
    case MaterializeView(mode) => {
      if (RESET_TRANSFORMATION_CHECKSUMS == mode) {
        val before = versionChecksum
        setVersion(view)
        log.info(s"VIEWACTOR CHECKSUM RESET ===> before=${before} , after=${versionChecksum}")
      }
      if (view.dependencies.isEmpty) {
        sender ! ViewMaterialized(view, incomplete, lastTransformationTimestamp, withErrors)
      } else {
        toWaiting(mode)
      }
    }

    case Invalidate() => {
      sender ! ViewStatusResponse("invalidated", view, self)
      toDefault(true, "invalidated")
    }
  })

  // State: failed, view actor failed to materialize
  // transitions:  default, transforming
  def failed: Receive = LoggingReceive({
    case Invalidate() => {
      sender ! ViewStatusResponse("invalidated", view, self)
      toDefault(true, "invalidated")
    }

    case MaterializeView(mode) => sender ! Failed(view)
  })

  def aDependencyAnswered(dependency: org.schedoscope.dsl.View, mode: MaterializeViewMode) {
    dependenciesMaterializing.remove(dependency)

    if (!dependenciesMaterializing.isEmpty) {
      log.debug(s"This actor is still waiting for ${dependenciesMaterializing.size} dependencies, dependencyFreshness=${dependenciesFreshness}, incomplete=${incomplete}, dependencies with data=${oneDependencyReturnedData}")
      return
    }

    if (oneDependencyReturnedData) {
      if ((lastTransformationTimestamp <= dependenciesFreshness) || hasVersionMismatch(view)) {
        if (lastTransformationTimestamp <= dependenciesFreshness)
          log.debug(s"Initiating transformation because of timestamp difference: ${lastTransformationTimestamp} <= ${dependenciesFreshness}")

        if (hasVersionMismatch(view))
          log.debug(s"Initiating transformation because of transformation checksum difference: ${view.transformation().checksum} != ${versionChecksum}")

        toTransformingOrMaterialized(0, mode)
      } else {
        toMaterialized()
      }
    } else {
      listenersWaitingForMaterialize.foreach(s => {
        log.debug(s"sending NoDataAvailable to ${s}"); s ! NoDataAvailable(view)
      })
      listenersWaitingForMaterialize.clear

      toDefault(false, "nodata")
    }
  }

  def prepareCurrentDependencies = {
    val currentDependencies = view.dependencies.toSet

    if (currentDependencies.size != knownDependencies.size) {

      log.info(s"Encountered new dependencies for view ${view}")

      currentDependencies.diff(knownDependencies).foreach { d =>

        log.info(s"Asking view manager actor to prepare view actor for new dependency ${d}")

        queryActor[ActorRef](viewManagerActor, d, settings.viewManagerResponseTimeout)
      }

      knownDependencies = currentDependencies
    }

    currentDependencies
  }

  def toWaiting(mode: MaterializeViewMode) {
    withErrors = false
    incomplete = false
    dependenciesFreshness = 0l

    listenersWaitingForMaterialize.add(sender)

    logStateInfo("waiting")

    val dependencies = prepareCurrentDependencies

    dependencies.foreach { d =>
      {
        dependenciesMaterializing.add(d)

        log.debug("sending materialize to dependency " + d)

        ViewManagerActor.actorForView(d) ! MaterializeView(mode)
      }
    }

    become(waiting(mode))
  }

  def toDefault(invalidate: Boolean = false, state: String = "receive") {
    lastTransformationTimestamp = if (invalidate) -1l else 0l
    dependenciesFreshness = 0l
    withErrors = false
    incomplete = false

    logStateInfo(state)

    become(receive)
  }

  def toMaterialized() {
    logStateInfo("materialized")

    listenersWaitingForMaterialize.foreach(s => s ! ViewMaterialized(view, incomplete, lastTransformationTimestamp, withErrors))
    listenersWaitingForMaterialize.clear

    become(materialized)

    oneDependencyReturnedData = false
    dependenciesFreshness = 0l
  }

  def toTransformingOrMaterialized(retries: Int, mode: MaterializeViewMode) {
    if (view.isMaterializeOnce && lastTransformationTimestamp > 0l && mode != RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS) {
      log.debug("materializeOnce for " + view + " set and view already materialized. Not materializing again")

      toMaterialized()
    } else view.transformation() match {
      case NoOp() => {
        if (successFlagExists(view) && view.dependencies.isEmpty) {
          log.debug("no dependencies for " + view + ", success flag exists, and no transformation specified")
          setVersion(view)
          getOrLogTransformationTimestamp(view)

          toMaterialized()
        } else if (!view.dependencies.isEmpty) {
          log.debug("dependencies for " + view + ", and no transformation specified")
          setVersion(view)
          getOrLogTransformationTimestamp(view)

          toMaterialized()
        } else {
          log.debug("no data and no dependencies for " + view)

          listenersWaitingForMaterialize.foreach(s => s ! NoDataAvailable(view))
          listenersWaitingForMaterialize.clear

          toDefault(false, "nodata")
        }
      }

      case _: FilesystemTransformation => {
        log.debug(s"FileTransformation: lastTransformationTimestamp ${lastTransformationTimestamp}, ")
        if (lastTransformationTimestamp > 0l && viewDirectorySize > 0l)
          toMaterialized()
        else
          toTransforming(retries, mode)
      }

      case _ => toTransforming(retries, mode)
    }
  }

  def toTransforming(retries: Int, mode: MaterializeViewMode) {
    if (mode != RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS)
      transformationManagerActor ! view
    else
      log.info(s"VIEWACTOR CHECKSUM AND TIMESTAMP RESET ===> Ignoring transformation")

    logStateInfo("transforming")

    become(transforming(retries, mode))

    if (mode == RESET_TRANSFORMATION_CHECKSUMS_AND_TIMESTAMPS) {
      self ! TransformationSuccess[NoOp](null, null)
      log.info(s"VIEWACTOR CHECKSUM AND TIMESTAMP RESET ===> Faking successful transformation action result")
    }
  }

  def toRetrying(retries: Int, mode: MaterializeViewMode): akka.actor.Cancellable = {
    logStateInfo("retrying")

    become(retrying(retries, mode))

    // exponential backoff
    system.scheduler.scheduleOnce(Duration.create(Math.pow(2, retries).toLong, "seconds"))(self ! Retry())
  }

  def toDefaultAndReload() {
    toDefault()

    self ! MaterializeView()
  }

  val hdfs = defaultFileSystem(settings.hadoopConf)

  def successFlagExists(view: View): Boolean =
    settings.userGroupInformation.doAs(new PrivilegedAction[Boolean]() {
      def run() = {
        val pathWithSuccessFlag = new Path(view.fullPath + "/_SUCCESS")

        hdfs.exists(pathWithSuccessFlag)
      }
    })

  // Calculate size of view data
  def viewDirectorySize = {
    val size = settings.userGroupInformation.doAs(new PrivilegedAction[Long]() {
      def run() = {
        val path = new Path(view.fullPath)
        val files = hdfs.listStatus(path, new PathFilter() {
          def accept(p: Path): Boolean = !p.getName().startsWith("_")

        })

        files.foldLeft(0l) { (size, status) => size + status.getLen() }
      }
    })
    size
  }

  def touchSuccessFlag(view: View) {
    transformationManagerActor ! Touch(view.fullPath + "/_SUCCESS")
  }

  def hasVersionMismatch(view: View) = view.transformation().checksum != versionChecksum

  def logTransformationTimestamp(view: View) = {
    lastTransformationTimestamp = new Date().getTime()
    metadataLoggerActor ! LogTransformationTimestamp(view, lastTransformationTimestamp)
    lastTransformationTimestamp
  }

  def getOrLogTransformationTimestamp(view: View) = {
    val ts = lastTransformationTimestamp
    if (ts <= 0l)
      logTransformationTimestamp(view)
    else ts
  }

  def setVersion(view: View) {
    versionChecksum = view.transformation().checksum
    metadataLoggerActor ! SetViewVersion(view)
  }

  def logStateInfo(stateName: String, toViewManager: Boolean = true) {
    if (toViewManager) viewManagerActor ! ViewStatusResponse(stateName, view, self)

    log.info(s"VIEWACTOR STATE CHANGE ===> ${stateName.toUpperCase()}: lastTransformationTimestamp=${lastTransformationTimestamp} versionChecksum=${versionChecksum} dependenciesFreshness=${dependenciesFreshness} incomplete=${incomplete} withErrors=${withErrors}")
  }
}

object ViewActor {
  def props(view: View, settings: SchedoscopeSettings, viewManagerActor: ActorRef, transformationManagerActor: ActorRef, metadataLoggerActor: ActorRef, versionChecksum: String = null, lastTransformationTimestamp: Long = 0l, viewDispatcher: String = null): Props = Props(classOf[ViewActor], view, settings, viewManagerActor, transformationManagerActor, metadataLoggerActor, versionChecksum, lastTransformationTimestamp).withDispatcher(s"akka.actor.views-dispatcher")
}
