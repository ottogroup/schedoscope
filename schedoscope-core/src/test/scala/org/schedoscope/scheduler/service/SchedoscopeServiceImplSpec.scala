package org.schedoscope.scheduler.service

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.scheduler.messages.{GetViews, ViewStatusListResponse, ViewStatusResponse}
import org.schedoscope.{Schedoscope, Settings, TestUtils}
import test.extviews.ExternalShop
import test.views.{Brand, ViewWithExternalDeps}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
    override lazy val settings = TestUtils.createSettings("schedoscope.external-dependencies.enabled=true",
      """schedoscope.external-dependencies.home=["${env}.test.views"] """ )
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
    val testView = ExternalShop()

    the [IllegalArgumentException] thrownBy {
      service.views(Some(testView.urlPath), None, None, None, None, None)
    } should have message "Invalid view URL pattern passed: test.extviews/Shop/.\n" +
      "original Message: You can not address an external view directly."
  }

  it should "allow a call on an internal view" in new SchedoscopeServiceExternalTest {
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
}
