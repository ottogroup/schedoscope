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

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, ExtensionId, ExtensionIdProvider}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.Config
import org.joda.time.LocalDateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.{Schedoscope, Settings}
import org.schedoscope.dsl.View
import org.schedoscope.dsl.Parameter._
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states._

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import test.views.Brand

class ViewSchedulingListenerManagerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  val TIMEOUT = 5 seconds

  trait viewSchedulingListenerManagerActorTest {
    implicit val timeout = Timeout(TIMEOUT)

    lazy val settings = Settings()

    val schemaManagerRouter = TestProbe()
    val actionsManagerActor = TestProbe()
    val viewSchedulingListenerManagerActor = TestProbe()

    val viewManagerActor = TestActorRef(ViewManagerActor.props(
      settings,
      actionsManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))
    val transformationManagerActor = TestProbe()

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor
    val view = Brand(p("ec01"))

    def initializeView(view: View): ActorRef = {

      val future = viewManagerActor ? view

      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(AddPartitions(List(view)))
      schemaManagerRouter.reply(TransformationMetadata(Map(view -> ("test", 1L))))

      Await.result(future, TIMEOUT)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }
  }

  "A ViewActor" should "send a ViewSchedulingEvent msg to the viewSchedulingListenerManagerActor" in
    new viewSchedulingListenerManagerActorTest {

    val brandViewActor = initializeView(view)

    val futureMaterialize = brandViewActor ? MaterializeView()

    viewSchedulingListenerManagerActor.expectMsgPF() {
      case ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime) => {
        prevState shouldBe ReadFromSchemaManager(view, "test", 1L)
        newState.label shouldBe "transforming"
        newState.view shouldBe view
        actions.head.isInstanceOf[Transform] shouldBe true
      }
    }

  }

  it should "cache views along with their last event and reply to listeners that have been restarted" in {

    implicit val timeout = Timeout(TIMEOUT)

    val view = Brand(p("ec01"))

    val viewSchedulingListenerManagerActor = TestActorRef(
      ViewSchedulingListenerManagerActor.props(
        Schedoscope.settings))

    // for the sake of coherence with the real process, let's use a fake
    // viewActor and another fake listenerHandlerActor
    val viewActor = TestProbe()
    val listenerHandlerActor = TestProbe()

    val prevState1 = CreatedByViewManager(view)

    viewActor.send(viewSchedulingListenerManagerActor,
      ViewSchedulingMonitoringEvent(prevState1, prevState1, Set(Transform(view)), new LocalDateTime()))

    // register handler so it can collect view Scheduling status from cache
    val handlerClassName = "someHandlerClassName"
    listenerHandlerActor.send(viewSchedulingListenerManagerActor, RegisterFailedListener(handlerClassName))
    listenerHandlerActor.send(viewSchedulingListenerManagerActor, CollectViewSchedulingStatus(handlerClassName))

    listenerHandlerActor.expectMsgPF() {
      case ViewSchedulingMonitoringEvent(prevState, newState, actions, eventTime) => {
        prevState shouldBe prevState1
        newState shouldBe prevState1
        actions.head shouldBe Transform(view)
      }
    }

  }


  it should "initialize listeners, and be able to recover each view's latest event" in {

    val handlerClassName = "org.schedoscope.test.TestViewListener"

    class SchedoscopeSettingsMock(config: Config) extends SchedoscopeSettings(config: Config) {
      override lazy val viewSchedulingListeners = List(handlerClassName)
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
    // confirm if initialized external listener
    viewSchedulingListenerManagerActor.children.size shouldBe 1

    val viewActor = TestProbe()
    val fakeChild = TestProbe()
    val view = Brand(p("ec01"))

    val prevState = CreatedByViewManager(view)
    val newState = Failed(view)
    val event = ViewSchedulingMonitoringEvent(prevState, newState, Set(Transform(view)), new LocalDateTime())

    viewActor.send(viewSchedulingListenerManagerActor, event)

    fakeChild.send(viewSchedulingListenerManagerActor, RegisterFailedListener(handlerClassName))

    fakeChild.send(viewSchedulingListenerManagerActor, CollectViewSchedulingStatus(handlerClassName))

    // confirm that on restart an actor could receive again latest events
    fakeChild.expectMsg(event)
  }


}
