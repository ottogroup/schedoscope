package com.ottogroup.bi.soda.bottler

import akka.actor.Props
import akka.actor.Actor
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import scala.concurrent._
import akka.event.Logging
import java.sql.SQLException
import org.joda.time.LocalDateTime
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import org.apache.thrift.transport.TTransportException

class HiveActor(ds: DriverSettings) extends Actor {
  val hiveDriver = HiveDriver(ds)
  import context._
  val ec = ExecutionContext.global
  val log = Logging(system, this)
  var startTime = new LocalDateTime()

  def running(sql: String): Receive = {
    case "tick" =>
    case _: GetStatus => sender() ! new HiveStatusResponse("executing query", self, ProcessStatus.RUNNING, sql, startTime)
    case CommandWithSender(_: KillAction, s) =>
  }

  override def receive: Receive = {
    case WorkAvailable => sender ! PollCommand("hive")
    case CommandWithSender(d: Deploy, s) => hiveDriver.deployAll(ds)
    case CommandWithSender(h: HiveTransformation, s) => {
      val actionsRouter = sender
      val requester = s
      val f = future {
        startTime = new LocalDateTime()
        hiveDriver.runAndWait(h)
      }(ec)
      f.onSuccess {
        case r => {
          requester ! r
          finish(receive, actionsRouter)
        }
      }
      f.onFailure {
        case e => throw e
      }
      become(running(h.sql))
    }
    case _: GetStatus => sender ! HiveStatusResponse("idle", self, ProcessStatus.IDLE, "", startTime)

  }

  private def finish(receive: => HiveActor.this.Receive, actionsRouter: akka.actor.ActorRef): Unit = {
    unbecome
    become(receive)
    startTime = new LocalDateTime()
    actionsRouter ! PollCommand("hive")
  }

}

object HiveActor {
  def props(ds: DriverSettings) = Props(new HiveActor(ds))
}