package com.ottogroup.bi.soda.bottler

import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging
import akka.pattern.{ ask, pipe }
import com.ottogroup.bi.soda.dsl.View
import akka.actor._
import akka.util.Timeout
import scala.concurrent.Await
import com.ottogroup.bi.soda.crate.ddl.HiveQl._
import com.ottogroup.bi.soda.dsl.NoOp
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.FileSystem
import java.security.PrivilegedAction
import scala.concurrent.Future
import akka.event.LoggingReceive
import scala.concurrent.duration._
import com.ottogroup.bi.soda.dsl.Parameter
import java.util.Properties
import org.apache.oozie.client.OozieClient
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import java.io.FileWriter
import java.io.File
import java.io.FileOutputStream
import com.ottogroup.bi.soda.bottler.driver.OozieDriver._
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Touch
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Delete
import java.util.concurrent.TimeoutException
import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import akka.contrib.pattern.Aggregator
import com.ottogroup.bi.soda.dsl.NoOp
import com.ottogroup.bi.soda.bottler.driver.DriverRunSucceeded
import com.ottogroup.bi.soda.bottler.driver.DriverRunFailed

class ViewStatusRetriever extends Actor with Aggregator {

  expectOnce {
    case GetStatus() => new MultipleResponseHandler(sender)
  }

  class MultipleResponseHandler(originalSender: ActorRef) {

    import context.dispatcher
    import collection.mutable.ArrayBuffer

    val values = ArrayBuffer.empty[ViewStatusResponse]

    context.actorSelection("/user/supervisor/*") ! GetStatus()
    context.system.scheduler.scheduleOnce(50 milliseconds, self, TimedOut)

    val handle = expect {
      case ar: ViewStatusResponse => values += ar
      case TimedOut => processFinal(values.toList)
    }

    def processFinal(eval: List[ViewStatusResponse]) {
      unexpect(handle)
      //val result = eval.foldLeft(collection.mutable.Map[String, Long]()) { (map, vsr) => map += (vsr.state -> (map.getOrElse(vsr.state, 0l) + 1)) }
      originalSender ! eval
      context.stop(self)
    }
  }
}

class ViewSuperVisor( settings:SettingsImpl) extends Actor {
  import context._
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import scala.concurrent.duration._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: ArithmeticException => Resume
      case _: NullPointerException => Restart
      case _: IllegalArgumentException => Stop
      case _: TimeoutException => Resume
      case _: Exception => Escalate
    }

  def receive = {
    case v: View => {
      //generate a unique id for every actor
      val actorName = v.module + v.n + v.parameters.foldLeft("") { (s, p) => s"${s}+${p.n}=${p.v.get}" }

      val actor = actorFor(actorName)
      sender ! (if (actor.isTerminated)
        actorOf(ViewActor.props(v, settings), actorName)
      else
        actor)
    }
  }
}

class ViewActor(val view: View, val settings: SettingsImpl) extends Actor {
  import context._
  val log = Logging(system, this)
  implicit val timeout = new Timeout(settings.dependencyTimout)
  val hadoopConf = settings.hadoopConf
  val supervisor = actorFor("/user/supervisor")
  val schemaActor = actorFor("/user/schemaActor")
  val listeners = collection.mutable.Queue[ActorRef]()
  val dependencies = collection.mutable.HashSet[View]()
  val actionsRouter = actorFor("/user/actions")

  // state variables
  // one of the dependencies was not available (no data)
  var incomplete = false

  // one of the dependencies or the view itself was recreated
  var changed = false

  // one of the dependencies' transformations failed
  var withErrors=false
  var availableDependencies = 0

  def receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("receive", view)
    case "materialize" => materializeDependencies
  })

  def transform(retries: Int) = {
    val partitionResult = schemaActor ? AddPartition(view)
    Await.result(partitionResult, settings.schemaActionTimeout)
    Await.result(actionsRouter ? Delete(view.fullPath, true), settings.fileActionTimeout)

    actionsRouter ! view

    unbecome()
    become(transforming(retries))

    log.debug("STATE CHANGE:transforming")
  }
  
  // State: transforming
  // transitions: materialized,failed,transforming
  def transforming(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("transforming", view)

    case _: ActionSuccess[_] => {
      log.info("SUCCESS")

      Await.result(actionsRouter ? Touch(view.fullPath + "/_SUCCESS"), settings.fileActionTimeout)
      Await.result(schemaActor ? SetVersion(view), settings.schemaActionTimeout)

      unbecome()
      become(materialized)

      log.debug("STATE CHANGE:transforming->materialized")

      listeners.foreach(s => { log.debug(s"sending VMI to ${s}"); s ! ViewMaterialized(view, incomplete, true, withErrors) })
      listeners.clear
    }

    case _: ActionFailure[_] | _: ActionExceptionFailure[_] => retry(retries)
  })

  def reload() = {
    unbecome()
    become(waiting)

    log.debug("STATE CHANGE:waiting")

    Await.result(actionsRouter ? Delete(view.fullPath + "/_SUCCESS", false), settings.fileActionTimeout)
    transform(1)

    // tell everyone that new data is avaiable
    system.actorSelection("/user/supervisor/*") ! NewDataAvailable(view)
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

    case "materialize" => listeners.enqueue(sender)

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

        FileSystem.get(hadoopConf).exists(pathWithSuccessFlag)
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

          val actor = Await.result((supervisor ? dependendView).mapTo[ActorRef], timeout.duration)
          dependencies.add(dependendView)
          availableDependencies += 1

          actor ! "materialize"
        }
      }

      become(waiting)

      log.debug("STATE CHANGE:receive -> waiting")
    }

    listeners.enqueue(sender)
  }
}

object ViewActor {
  def props(view: View,  settings:SettingsImpl): Props = Props(new ViewActor(view, settings))

}
object ViewSuperVisor {
  def props(settings:SettingsImpl): Props = Props(new ViewSuperVisor(settings:SettingsImpl))

}

