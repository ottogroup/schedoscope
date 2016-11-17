package org.schedoscope.scheduler.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.mock.MockitoSugar
import org.schedoscope.{Schedoscope, Settings}
import test.views.Brand
import org.schedoscope.dsl.Parameter._
import org.schedoscope.scheduler.actors.ViewManagerActor
import org.schedoscope.scheduler.messages.{GetViews, ViewStatusListResponse, ViewStatusResponse}
import java.util.concurrent._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class SchedoscopeServiceImplSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }



  trait SchedoscopeServiceTest {

    lazy val settings = Settings()
    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestProbe()
    val transformationManagerActor = TestProbe()
    Schedoscope.viewManagerActorBuilder = () => viewManagerActor.ref

    lazy val service = new SchedoscopeServiceImpl(system,
      settings,
      viewManagerActor.ref,
      transformationManagerActor.ref)
  }

  trait SchedoscopeServiceExternalTest extends SchedoscopeServiceTest {
    val myConfig =
      ConfigFactory.parseString("schedoscope.external.enabled=true\n" +
        """schedoscope.external.internal=["prod.app.test"] """ )
    // load the normal config stack (system props,
    // then application.conf, then reference.conf)
    val regularConfig =
    ConfigFactory.load()
    // override regular stack with myConfig
    val combined =
    myConfig.withFallback(regularConfig)
    // put the result in between the overrides
    // (system props) and defaults again
    val complete = ConfigFactory.load(combined)

    override lazy val settings = Settings(complete)
  }

  "the service" should "ask for status" in new SchedoscopeServiceTest {
    val testView = Brand(p("test"))
    val response = Future {
      service.views(Some(testView.urlPath), None, None, None, None, None)
    }
    viewManagerActor.expectMsg(GetViews(Some(List(testView)), None, None))
    viewManagerActor.reply(ViewStatusListResponse(List(ViewStatusResponse("loading", testView, viewManagerActor.ref))))

    val expected = ViewStatusList(Map("loading" -> 1),
      List(ViewStatus("test.views/Brand/test",
        None, "loading", None, None, None, None, None, None, None, None, None, None)))

    whenReady(response) { result =>
      result shouldBe expected
    }
  }

  it should "block a call on an external view" in new SchedoscopeServiceExternalTest {
    val testView = Brand(p("test"))

    the [IllegalArgumentException] thrownBy {
      service.views(Some(testView.urlPath), None, None, None, None, None)
    } should have message "Invalid view URL pattern passed: test.views/Brand/test.\n" +
      "original Message: You can not access an external view directly"


  }



}
