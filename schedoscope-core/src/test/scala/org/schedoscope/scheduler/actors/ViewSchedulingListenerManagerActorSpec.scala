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

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, ExtensionId, ExtensionIdProvider, Props}
import akka.testkit.EventFilter
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.Config
import org.joda.time.LocalDateTime

import org.scalatest.mock.MockitoSugar

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.{Schedoscope, Settings}
import org.schedoscope.dsl.View
import org.schedoscope.dsl.Parameter._
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states.{Materialize, ReportNoDataAvailable}

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

import test.views.Brand

class ViewSchedulingListenerManagerActorSpec extends TestKit(ActorSystem("schedoscope",
  ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  val TIMEOUT = 5 seconds

  trait viewSchedulingListenerManagerActorTest {

    implicit val timeout = Timeout(TIMEOUT)

    val schemaManagerRouter = TestProbe()
    val transformationManagerActor = TestProbe()
    val viewSchedulingListenerManagerActor = TestProbe()

    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestActorRef(
      ViewManagerActor.props(
        Schedoscope.settings,
        transformationManagerActor.ref,
        schemaManagerRouter.ref,
        viewSchedulingListenerManagerActor.ref))

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor


    def initializeView(view: View, listeners:Boolean): ActorRef = {

      val future = viewManagerActor ? view
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(AddPartitions(List(view)))
      schemaManagerRouter.reply(TransformationMetadata(Map(view -> ("test", 1L))))

      // 2 main tests
      viewSchedulingListenerManagerActor.expectMsgPF() {
        case ViewSchedulingNewEvent(
        view, _, None, None, "receive") => ()
      }

      viewSchedulingListenerManagerActor.expectMsg(ViewSchedulingListenersExist(true))
      // reply true to make sure view
      viewSchedulingListenerManagerActor.reply(ViewSchedulingListenersExist(listeners))

      Await.result(future, TIMEOUT)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }
  }

  "A new initialized ViewActor" should "send 2 msgs to the viewSchedulingListenerManagerActor" in
    new viewSchedulingListenerManagerActorTest {

    val view = Brand(p("ec01"))
    val brandViewActor = initializeView(view, true)
  }

  "A ViewActor" should "not send msgs to the viewSchedulingListenerManagerActor " +
    "if there are no listening handlers" in new viewSchedulingListenerManagerActorTest {

    val view = Brand(p("ec01"))

    val brandViewActor = initializeView(view, false)
    val futureMaterialize = brandViewActor ? MaterializeView()

    viewSchedulingListenerManagerActor.expectNoMsg(TIMEOUT)
  }

  "A ViewActor" should "send a ViewSchedulingNewEvent msg to the viewSchedulingListenerManagerActor " +
    "upon state change (if there are handlers listening)" in new viewSchedulingListenerManagerActorTest {

    val view = Brand(p("ec01"))
    val brandViewActor = initializeView(view, true)

    val futureMaterialize = brandViewActor ? MaterializeView()

    viewSchedulingListenerManagerActor.expectMsgPF() {
      case ViewSchedulingNewEvent(
      brandView, _, Some(ReportNoDataAvailable(view, _)), Some("receive"), "nodata") => ()
    }

    Await.result(futureMaterialize, TIMEOUT)
    futureMaterialize.isCompleted shouldBe true
    futureMaterialize.value.get.isSuccess shouldBe true
  }

  "A viewSchedulingListenerManagerActor" should "cache views and their events" +
    "upon state change (if there are handlers listening)" in {

    implicit val timeout = Timeout(TIMEOUT)

    val view = Brand(p("ec01"))

    val viewSchedulingListenerManagerActor = TestActorRef(
      ViewSchedulingListenerManagerActor.props(
        Schedoscope.settings))

    // for the sake of coherence with the real process, let's use a fake
    // viewActor and another fake listenerHandlerActor
    val viewActor = TestProbe()
    val listenerHandlerActor = TestProbe()

    viewActor.send(viewSchedulingListenerManagerActor, ViewSchedulingNewEvent(
      view, new LocalDateTime(), None, None, "receive"))

    listenerHandlerActor.send(viewSchedulingListenerManagerActor, CollectViewSchedulingStatus())

    listenerHandlerActor.expectMsgPF() {
      case ViewSchedulingNewEvent(view, _, None, None, "receive") => ()
    }
  }

  "A viewSchedulingListenerManager" should "initialize successfully an external handler " +
    "class, and execute its methods" in {

    class SchedoscopeSettingsMock(config: Config) extends SchedoscopeSettings(config: Config) {
      override lazy val viewSchedulingRunCompletionHandlers = List("org.schedoscope.test.TestViewListenerHandler")
    }

    object SettingsMock extends ExtensionId[SchedoscopeSettings] with ExtensionIdProvider {
      override def lookup = Settings

      override def createExtension(system: ExtendedActorSystem) =
        new SchedoscopeSettings(system.settings.config)

      override def get(system: ActorSystem): SchedoscopeSettings = super.get(system)

      def apply() = {
        super.apply(Schedoscope.actorSystem)
      }

      def apply(config: Config) =
        new SchedoscopeSettingsMock(config)
    }

    val viewSchedulingListenerManagerActor = TestActorRef(
      ViewSchedulingListenerManagerActor.props(
        SettingsMock(ConfigFactory.load())))

    val viewActor = TestProbe()
    val view = Brand(p("ec01"))

    viewActor.send(viewSchedulingListenerManagerActor, ViewSchedulingListenersExist(true))
    viewActor.expectMsg(ViewSchedulingListenersExist(true))

    // state change test
    EventFilter.info(pattern = "Cool, it works well!*", occurrences = 1) intercept {
      viewActor.send(viewSchedulingListenerManagerActor, ViewSchedulingNewEvent(
        view, new LocalDateTime(), None, None, "receive"))
    }

    // no state change test
    EventFilter.info(pattern = "And the second too, we're on a lucky streak!*", occurrences = 1) intercept {
      viewActor.send(viewSchedulingListenerManagerActor, ViewSchedulingNewEvent(
        view, new LocalDateTime(), None, Some("receive"), "receive"))
    }

  }


}
