package org.schedoscope.scheduler.utils

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class BackOffSupervisionTest extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with BeforeAndAfterAll
  with Matchers {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val managedActor = TestProbe()

  it should "send an immediate tick to a actor that booted for the first time" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val backOffSlot = 100 millis
    val backOffMinDelay = 10 millis

    val tickTime = bos.manageActorLifecycle(managedActor.ref, backOffSlot, backOffMinDelay)
    system.scheduler.scheduleOnce(tickTime, managedActor.ref, "tick")
    managedActor.expectMsg(max = 1 seconds, "tick")
  }

  it should "schedule a tick for an actor that is rebooting" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val backOffSlot = 100 millis
    val backOffMinDelay = 1 seconds

    val tickTime = bos.manageActorLifecycle(managedActor.ref, backOffSlot, backOffMinDelay)
    system.scheduler.scheduleOnce(tickTime, managedActor.ref, "tick")

    managedActor.expectMsg(max = 1 seconds, "tick")

    val tickTime2 = bos.manageActorLifecycle(managedActor.ref)
    system.scheduler.scheduleOnce(tickTime2, managedActor.ref, "tick")

    managedActor.expectMsg(max = 3 seconds, "tick")
  }

  it should "schedule a tick for an actor that is rebooting " +
    "(assure that tick is happening in the future only)" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val backOffSlot = 100 millis
    val backOffMinDelay = 5 seconds

    val tickTime = bos.manageActorLifecycle(managedActor.ref, backOffSlot, backOffMinDelay)
    system.scheduler.scheduleOnce(tickTime, managedActor.ref, "tick")
    managedActor.expectMsg(max = 1 seconds, "tick")


    val tickTime2 = bos.manageActorLifecycle(managedActor.ref)
    system.scheduler.scheduleOnce(tickTime2, managedActor.ref, "tick")
    managedActor.expectNoMsg(max = 3 seconds)
  }

}
