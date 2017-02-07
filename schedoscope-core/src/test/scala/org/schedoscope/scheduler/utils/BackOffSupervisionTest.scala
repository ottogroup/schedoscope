package org.schedoscope.scheduler.utils

import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import akka.testkit.{TestActorRef, TestKit, TestProbe}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class BackOffSupervisionTest extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with BeforeAndAfterAll
  with Matchers
{

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val managedActor = TestProbe()

  it should "schedule a tick for a given specific actor" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val schedule = 100 millis

    bos.scheduleTick(managedActor.ref, schedule)
    managedActor.expectMsg(max = 1 seconds, "tick")
  }

  it should "send an immediate tick to a actor that booted for the first time" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val backOffSlot = 100 millis
    val backOffMinDelay = 10 millis

    bos.manageActorLifecycle(managedActor.ref, backOffSlot, backOffMinDelay)
    managedActor.expectMsg(max = 1 seconds, "tick")
  }

  it should "schedule a tick for an actor that is rebooting" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val backOffSlot = 100 millis
    val backOffMinDelay =  1 seconds

    bos.manageActorLifecycle(managedActor.ref, backOffSlot, backOffMinDelay)
    managedActor.expectMsg(max = 1 seconds, "tick")

    bos.manageActorLifecycle(managedActor.ref)
    managedActor.expectMsg(max = 3 seconds, "tick")
  }

  it should "schedule a tick for an actor that is rebooting " +
    "(assure that tick is happening in the future only)" in {
    val bos = new BackOffSupervision("THE MANAGER", system)
    val backOffSlot = 100 millis
    val backOffMinDelay =  5 seconds

    bos.manageActorLifecycle(managedActor.ref, backOffSlot, backOffMinDelay)
    managedActor.expectMsg(max = 1 seconds, "tick")

    bos.manageActorLifecycle(managedActor.ref)
    managedActor.expectNoMsg(max = 3 seconds)
  }

}
