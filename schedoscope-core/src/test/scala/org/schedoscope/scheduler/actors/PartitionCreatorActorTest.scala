package org.schedoscope.scheduler.actors

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Schedoscope
import akka.testkit.EventFilter
import com.typesafe.config.ConfigFactory

class PartitionCreatorActorTest extends TestKit(ActorSystem("schedoscope",
  ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val msgHub = TestProbe()
  val settings = Schedoscope.settings


  case class ToPCA(msg: String)

  class TestRouter(to: ActorRef) extends Actor {
    val pca = TestActorRef(new PartitionCreatorActor("","","") {
      override def getSchemaManager(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = {
        null
      }
      override def schemaRouter = msgHub.ref
    })

    def receive = {
      case ToPCA(m) => pca forward(m)

      case "tick" => to forward "tick"
    }
  }

  it should "send tick msg upon start" in {
    TestActorRef(new TestRouter(msgHub.ref))
    msgHub.expectMsg("tick")
  }

  it should "change to active state upon receive of tick msg" in {
    val router = TestActorRef(new TestRouter(msgHub.ref))

    EventFilter.info(message="PARTITION CREATOR ACTOR: changed to active state.", occurrences = 1) intercept {
      msgHub.send(router, ToPCA("tick"))
    }
  }

}
