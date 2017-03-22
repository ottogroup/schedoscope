package org.schedoscope.scheduler.actors

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, FunSuite, Matchers}
import org.schedoscope.Schedoscope


class MetadataLoggerActorTest extends TestKit(ActorSystem("schedoscope",
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


  case class toPCA(msg: String)

  class TestRouter(to: ActorRef) extends Actor {
    val pca = TestActorRef(new MetadataLoggerActor("", "", "") {
      override def getSchemaManager(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = {
        null
      }

      override def schemaRouter = msgHub.ref
    })

    def receive = {
      case toPCA(m) => pca forward (m)

      case "tick" => to forward "tick"
    }
  }

  it should "send tick msg upon start" in {
    TestActorRef(new TestRouter(msgHub.ref))
    msgHub.expectMsg("tick")
  }

  it should "change to active state upon receive of tick msg" in {
    val router = TestActorRef(new TestRouter(msgHub.ref))

    EventFilter.info(message = "METADATA LOGGER ACTOR: changed to active state.", occurrences = 1) intercept {
      msgHub.send(router, toPCA("tick"))
    }
  }

}
