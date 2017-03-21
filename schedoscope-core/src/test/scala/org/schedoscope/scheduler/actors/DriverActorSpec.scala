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

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{EventFilter, TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import test.views.ProductBrand
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.scheduler.messages._
import org.schedoscope.schema.ddl.HiveQl
import org.schedoscope.Settings

import scala.util.Random
import scala.concurrent.duration._

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
