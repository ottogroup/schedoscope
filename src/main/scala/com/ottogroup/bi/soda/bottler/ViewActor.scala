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

class ViewSuperVisor(ugi: UserGroupInformation, hadoopConf: Configuration) extends Actor {
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
        actorOf(ViewActor.props(v, ugi, hadoopConf), actorName)
      else
        actor)
    }
  }
}

class ViewActor(val view: View, val ugi: UserGroupInformation, val hadoopConf: Configuration) extends Actor {

  val maxRetry = 5
  import context._
  val log = Logging(system, this)
  implicit val timeout = Timeout(2 day) // needed for `?` below
  val supervisor = actorFor("/user/supervisor")
  val schemaActor = actorFor("/user/schemaActor")
  val listeners = collection.mutable.Queue[ActorRef]()
  val dependencies = collection.mutable.HashSet[View]()
  val actionsRouter = actorFor("/user/actions")
  var incomplete = false
  var changed = false
  var availableDependencies = 0
  //log.info("new actor: " + view + " in " + view.env)

  def transform(retries: Int) = {
    import scala.reflect.ClassTag
    val partitionResult = schemaActor ? AddPartition(view)
    Await.result(partitionResult, Duration.create("10 minutes"))

    Await.result(actionsRouter ? Delete(view.fullPath, true), Duration.create("10 minutes"))
    actionsRouter ! view
    unbecome()
    become(transforming(retries))
    
  }
  
  // State: transforming
  // transitions: materialized,failed,transforming
  def transforming(retries:Int):Receive = {
     case _: OozieSuccess | _: HiveSuccess => {
        Await.result(actionsRouter ? Touch(view.fullPath+"/_SUCCESS"), 10 minute)
        Await.result(schemaActor ? SetVersion(view), 10 minute)
        become(materialized)
        log.info("got success")
        listeners.foreach(s => { log.debug(s"sending VMI to ${s}"); s ! ViewMaterialized(view, incomplete, true) })
        listeners.clear

      }
      case OozieException(exception) => retry(retries)
      case _: OozieError | _: HiveError | _: Error => retry(retries)
  }
  /**
   * reload
   *
   */
  def reload() = {
    unbecome()
    become(waiting)
    Await.result(actionsRouter ? Delete(view.fullPath + "/_SUCCESS", false), 10 minutes)
    transform(1)
    // tell everyone that new data is avaiable
    system.actorSelection("/user/supervisor/*") ! NewDataAvailable(view)
  }

  // State: materialized
  // transitions: receive,materialized,transforming
  def materialized: Receive = LoggingReceive({
    case "materialize" => sender ! ViewMaterialized(view, incomplete, false)
    case "invalidate" => unbecome();become(receive)
    case NewDataAvailable(view) => if (dependencies.contains(view)) reload()

  })

  // State: nodata
  // transitions; receive,transforming,nodata
  def nodata: Receive = LoggingReceive({
    case "materialize" => sender ! NoDataAvaiable(view)
    case "invalidate" => unbecome();become(receive)
    case NewDataAvailable(view) => if (dependencies.contains(view)) reload()

  })
  
  
  def retrying(retries: Int): Receive = {
    case "materialize" => listeners.add(sender)
    case "retry" => if (retries <= maxRetry)
      transform(retries + 1)
    else {
      unbecome();become(failed)
    }
  }

  def failed: Receive = {
        case NewDataAvailable(view) => if (dependencies.contains(view)) reload()
        case "invalidate" => unbecome();become(receive)
        case _ => sender ! FatalError(view, "not recoverable")
  }

  // waiting for depending tables to materialize
  def waiting: Receive = LoggingReceive {
    case "materialize" => listeners.enqueue(sender)

    case NoDataAvaiable(dependency) => {
      log.debug("received nodata from " + dependency); incomplete = true; availableDependencies -= 1; dependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, true, false) => {
      log.debug("incomplete,not changed from " + dependency); incomplete = true; dependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, false, false) => {
      log.debug("complete,not changed from " + dependency);
      dependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, true, true) => {
      log.debug("incomplete,changed from " + dependency);
      incomplete = true; changed = true; dependencyIsDone(dependency)
    }
    case ViewMaterialized(dependency, false, true) => {
      log.debug("complete, changed from " + dependency);
      changed = true; dependencyIsDone(dependency)
    }
  }

  def receive = LoggingReceive({
    case "materialize" => {
      materializeDependencies
    }
  })

  def successFlagExists(view: View): Boolean = {
    ugi.doAs(new PrivilegedAction[Boolean]() {
      def run() = {
        val pathWithSuccessFlag = new Path(view.fullPath+"/_SUCCESS")
        FileSystem.get(hadoopConf).exists(pathWithSuccessFlag)
      }
    })
  }

  private def retry(retries: Int): akka.actor.Cancellable = {
    become(retrying(retries))
    log.warning("failed, will retry")
    // exponential backoff
    system.scheduler.scheduleOnce(Duration.create(Math.pow(2, retries).toLong, "seconds"))(self ! "retry")
  }

  private def dependencyIsDone(dependency: com.ottogroup.bi.soda.dsl.View) = {
    dependencies.remove(dependency)
    log.debug(s"this actor still waits for ${dependencies.size} dependencies")
    if (dependencies.isEmpty) {
      // there where dependencies that did not return nodata
      if (availableDependencies > 0) {
        if (!changed) {
          val versionInfo = Await.result(schemaActor ? CheckVersion(view), 5 minutes)
          log.debug(versionInfo.toString)
          versionInfo match {
            case Error => changed = true
            case VersionOk(v) =>
            case VersionMismatch(v, version) => changed = true
          }
        }
        if (changed)
          transform(0)
        else
          listeners.foreach(s => s ! ViewMaterialized(view, incomplete, false))
      } else {
        listeners.foreach(s => { log.debug(s"sending NoDataAvailable to ${s}"); s ! NoDataAvaiable(view) })
        listeners.clear
        become(receive)
      }
    }
  }

  private def materializeDependencies: Unit = {
    log.info(view + " has dependencies " + view.dependencies)
    if (view.dependencies.isEmpty) {
      if (successFlagExists(view)) {
        log.info("success exists for " + view)
        sender ! ViewMaterialized(view, false, false)
        become(materialized)
      } else {
        log.info("no data and no dependencies for " + view)

        sender ! NoDataAvaiable(view)
        become(nodata)
      }
    } else {
      become(waiting)
      view.dependencies.foreach { dependendView =>
        {
          log.debug("querying dependency " + dependendView)
          val actor = Await.result((supervisor ? dependendView).mapTo[ActorRef], timeout.duration)
          dependencies.add(dependendView)
          availableDependencies += 1;
          actor ! "materialize"

        }
      }
    }
    listeners.enqueue(sender)
  }

}

object ViewActor {
  def props(view: View, ugi: UserGroupInformation, hadoopConf: Configuration): Props = Props(new ViewActor(view, ugi, hadoopConf))

}
object ViewSuperVisor {
  def props(ugi: UserGroupInformation, hadoopConf: Configuration): Props = Props(new ViewSuperVisor(ugi, hadoopConf))

}

