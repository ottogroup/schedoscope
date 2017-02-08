package org.schedoscope.scheduler.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.{Settings, TestUtils}
import scala.concurrent.duration._

class SchemaManagerRouterTest extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  // common vars
  val settings: SchedoscopeSettings = Settings()


  class ForwardChildActor(to: ActorRef) extends Actor {
    def receive = {
      case x => to.forward(x)
    }
  }

  trait SchemaManagerRouterTest {

    val partitionCreatorRouterActor = TestProbe()
    val metadataLoggerActorTest = TestProbe()

    def getSchemaManager(s: SchedoscopeSettings): ActorRef = {

      TestActorRef(new SchemaManagerRouter(s) {
        override def preStart{
          context.actorOf(Props(new ForwardChildActor(partitionCreatorRouterActor.ref)),
            "partition-creator")
          context.actorOf(Props(new ForwardChildActor(metadataLoggerActorTest.ref)),
            "metadata-logger")
        }
      })
    }
  }

  it should "should set an exponential backoff time for restarting drivers" in
    new SchemaManagerRouterTest {

      val newSettings = TestUtils.createSettings(
        "schedoscope.metastore.actor-backoff-slot-time=10",
        "schedoscope.metastore.actor-backoff-minimum-delay=0")


      var schemaManagerRouter: ActorRef = getSchemaManager(newSettings)

      partitionCreatorRouterActor.send(schemaManagerRouter, "tick")
      partitionCreatorRouterActor.expectMsg("tick")
      partitionCreatorRouterActor.send(schemaManagerRouter, "tick")
      partitionCreatorRouterActor.expectMsg("tick")

      metadataLoggerActorTest.send(schemaManagerRouter, "tick")
      metadataLoggerActorTest.expectMsg("tick")
      metadataLoggerActorTest.send(schemaManagerRouter, "tick")
      metadataLoggerActorTest.expectMsg("tick")

    }

  it should "should set an exponential backoff time too big for the test to get it" in
    new SchemaManagerRouterTest {

    val newSettings = TestUtils.createSettings(
      "schedoscope.metastore.actor-backoff-slot-time=10000",
      "schedoscope.metastore.actor-backoff-minimum-delay=10000")


    var schemaManagerRouter: ActorRef = getSchemaManager(newSettings)

    partitionCreatorRouterActor.send(schemaManagerRouter, "tick")
    partitionCreatorRouterActor.expectMsg("tick")
    partitionCreatorRouterActor.send(schemaManagerRouter, "tick")
    partitionCreatorRouterActor.expectNoMsg(3 seconds)

    metadataLoggerActorTest.send(schemaManagerRouter, "tick")
    metadataLoggerActorTest.expectMsg("tick")
    metadataLoggerActorTest.send(schemaManagerRouter, "tick")
    metadataLoggerActorTest.expectNoMsg(3 seconds)

  }

}
