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

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.routing.SmallestMailboxPool
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.scheduler.driver.FilesystemDriver.{apply => _, _}
import org.schedoscope.scheduler.driver.{Driver, FilesystemDriver, InvalidDriverClassException, RetryableDriverException}
import org.schedoscope.scheduler.messages._
import org.schedoscope.schema.ddl.HiveQl
import test.views.ProductBrand

import scala.concurrent.duration._
import scala.util.Random

class DriverActorSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  //common to all tests
  val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
  val settings = Settings()
  val transformationManagerActor = TestProbe()

  class BombActor(to: ActorRef, blastOnStart: Boolean = false, ex: Option[Exception] = None) extends Actor {

    override def receive: Receive = {

      case retryable: RetryableDriverException => to.forward("blasting on msg"); throw retryable

      case ie: IllegalArgumentException => to.forward("committing suicide..."); context stop self

      case "ref" => to.forward(self)

      case x => to.forward(x)
    }

    override def preStart() {
      if (blastOnStart) {
        to.forward("blasting on start")
        ex match {
          case Some(ex) => throw ex
          case _ => throw InvalidDriverClassException("boom goes the dynamite",
            new IllegalArgumentException)
        }
      }

    }
  }

  trait HiveActorTest {
    val hivedriverActor = TestActorRef(DriverActor.props(settings,
      "hive", transformationManagerActor.ref))
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "booted"
        actor shouldBe hivedriverActor
      }
    }
    transformationManagerActor.send(hivedriverActor, "tick")
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe hivedriverActor
      }
    }
  }

  trait DriverActorTest {
    val Seq(t1, t2, t3) = shuffleTransformations
    val driverActor1 = launchDriverActor(t1)
    val driverActor2 = launchDriverActor(t2)
    val driverActor3 = launchDriverActor(t3)

    def shuffleTransformations: Seq[String] = {
      val transformations = List("filesystem", "hive",
        "mapreduce", "noop", "seq")
      val q = Random.shuffle(transformations)
        .foldLeft(new collection.mutable.Queue[String]()) {
          (growingQueue, transformation) =>
            growingQueue += (transformation)
        }
      Seq(q.dequeue, q.dequeue, q.dequeue)
    }

    def launchDriverActor(transformation: String): ActorRef = {
      val driverActor = TestActorRef(DriverActor.props(settings,
        transformation, transformationManagerActor.ref))
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "booted"
          actor shouldBe driverActor
        }
      }
      transformationManagerActor.send(driverActor, "tick")
      transformationManagerActor.expectMsgPF() {
        case TransformationStatusResponse(msg, actor, driver,
        driverHandle, driverRunStatus) => {
          msg shouldBe "idle"
          actor shouldBe driverActor
        }
      }
      driverActor
    }
  }

/*  it should "NOT restart Driver actors upon exception thrown on ActorInitialization" in {
    val hackActor = TestProbe()

    val transformationManagerActor = system.actorOf(Props(new TransformationManagerActor(settings,
      bootstrapDriverActors = false) {
      override def preStart {
        val bombActor = context.actorOf(Props(new BombActor(hackActor.ref, true)))
        hackActor.watch(bombActor)
      }
    }))
    hackActor.expectMsg("blasting on start")
    hackActor.expectMsgPF() { case Terminated(t) => () }
  }*/

  it should "restart Driver routees upon RetryableDriverException" in {
    val hackActor = TestProbe()

    val driverRouterActor = system.actorOf(Props(new DriverActor[HiveTransformation](
      transformationManagerActor.ref,
      settings.getDriverSettings("hive"),
      (ds: DriverSettings) => Driver.driverFor(ds),
      5.seconds,
      settings,
      FilesystemDriver.defaultFileSystem(settings.hadoopConf)
    ) {
      override val supervisorStrategy = DriverActor.driverRouterSupervisorStrategy

      override def preStart {
        val bombActor1 = context.actorOf(Props(new BombActor(hackActor.ref, false)))
        hackActor.watch(bombActor1)
        hackActor.send(bombActor1, "ref")
      }
    })
    )

    val bombActor = hackActor.expectMsgType[ActorRef]
    hackActor.send(bombActor, RetryableDriverException("retry me"))
    hackActor.expectMsg("blasting on msg")
    hackActor.send(bombActor, RetryableDriverException("retry me"))
    hackActor.expectMsg("blasting on msg")
    hackActor.send(bombActor, RetryableDriverException("retry me"))
    hackActor.expectMsg("blasting on msg")
    hackActor.send(bombActor, RetryableDriverException("retry me"))
    hackActor.expectMsg("blasting on msg")
    hackActor.send(bombActor, new IllegalArgumentException("bam"))
    hackActor.expectMsg("committing suicide...")
    hackActor.expectMsgPF() { case Terminated(t) => () }
    hackActor.send(bombActor, RetryableDriverException("retry me"))
    hackActor.expectNoMsg()

  }

  it should "not execute commands before changing to activeReceive state" in {
    val hivedriverActor = TestActorRef(DriverActor.props(settings,
      "hive", transformationManagerActor.ref))
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "booted"
        actor shouldBe hivedriverActor
      }
    }
    val cmd = DriverCommand(DeployCommand(), transformationManagerActor.ref)
    transformationManagerActor.send(hivedriverActor, cmd)
    transformationManagerActor.expectNoMsg(3 seconds)
  }

  "All Driver Actors" should "run DeployCommand" in new DriverActorTest {
    val cmd = DriverCommand(DeployCommand(), transformationManagerActor.ref)
    transformationManagerActor.send(driverActor1, cmd)
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "deploy"
        actor shouldBe driverActor1
      }
    }
    transformationManagerActor.expectMsg(DeployCommandSuccess())
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe driverActor1
      }
    }

    transformationManagerActor.send(driverActor2, cmd)
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "deploy"
        actor shouldBe driverActor2
      }
    }
    transformationManagerActor.expectMsg(DeployCommandSuccess())
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe driverActor2
      }
    }

    transformationManagerActor.send(driverActor3, cmd)
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "deploy"
        actor shouldBe driverActor3
      }
    }
    transformationManagerActor.expectMsg(DeployCommandSuccess())
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe driverActor3
      }
    }

  }

  "HiveDriver" should "change state to run hive transformation, " +
    "kill it, and go back into idle state" in new HiveActorTest {

    val hiveTransformation = new HiveTransformation(HiveQl.ddl(view))
    val cmd = DriverCommand(TransformView(hiveTransformation, view),
      transformationManagerActor.ref)
    transformationManagerActor.send(hivedriverActor, cmd)
    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "running"
        actor shouldBe hivedriverActor
      }
    }
    // pseudo kill op
    transformationManagerActor.send(hivedriverActor, KillCommand())

    transformationManagerActor.expectMsgPF() {
      case TransformationStatusResponse(msg, actor, driver,
      driverHandle, driverRunStatus) => {
        msg shouldBe "idle"
        actor shouldBe hivedriverActor
      }
    }
  }

}
