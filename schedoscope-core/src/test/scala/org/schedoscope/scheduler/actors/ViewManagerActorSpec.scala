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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.{ExternalView, View}
import org.schedoscope.scheduler.messages._
import org.schedoscope.{Schedoscope, Settings, TestUtils}
import test.extviews.ExternalShop
import test.views._

import scala.concurrent.Await
import scala.concurrent.duration._

class ViewManagerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  trait ViewManagerActorTest {
    implicit val timeout = Timeout(5 seconds)

    lazy val settings = Settings()

    val schemaManagerRouter = TestProbe()
    val actionsManagerActor = TestProbe()

    val viewManagerActor = TestActorRef(ViewManagerActor.props(
      settings,
      actionsManagerActor.ref,
      schemaManagerRouter.ref))
    val transformationManagerActor = TestProbe()

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor
    val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val brandDependency = view.dependencies.head
    val productDependency = view.dependencies(1)

    def initializeView(view: View): ActorRef = {

      val future = viewManagerActor ? view

      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(productDependency)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(CheckOrCreateTables(List(brandDependency)))
      schemaManagerRouter.reply(SchemaActionSuccess())
      schemaManagerRouter.expectMsg(AddPartitions(List(view)))
      schemaManagerRouter.reply(TransformationMetadata(Map(view -> ("test", 1L))))
      schemaManagerRouter.expectMsg(AddPartitions(List(productDependency)))
      schemaManagerRouter.reply(TransformationMetadata(Map(productDependency -> ("test", 1L))))
      schemaManagerRouter.expectMsg(AddPartitions(List(brandDependency)))
      schemaManagerRouter.reply(TransformationMetadata(Map(brandDependency -> ("test", 1L))))

      Await.result(future, 5 seconds)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }
  }

  trait ViewManagerActorExternalTest extends ViewManagerActorTest {
    override lazy val settings = TestUtils.createSettings("schedoscope.external-dependencies.enabled=true",
      "schedoscope.external-dependencies.home=[\"dev.test.views\"]")
  }


  "The ViewManagerActor" should "create a new view" in new ViewManagerActorTest {
    initializeView(view)
  }

  it should "delegate a message to a view" in new ViewManagerActorTest {
    val actorRef = initializeView(view)

    viewManagerActor ! DelegateMessageToView(view, "test")

    expectMsg(NewViewActorRef(view, actorRef))
  }

  it should "delegate a message to a unknown view" in new ViewManagerActorTest {
    val unknownView = Brand(p("ec0101"))
    val actorRef = initializeView(view)
    viewManagerActor ! DelegateMessageToView(unknownView, "test")

    //if ViewManager does not know view it will start to communicate with
    //the SchemaManager
    schemaManagerRouter.expectMsg(CheckOrCreateTables(List(unknownView)))
    schemaManagerRouter.reply(SchemaActionSuccess())
    schemaManagerRouter.expectMsg(AddPartitions(List(unknownView)))
    schemaManagerRouter.reply(TransformationMetadata(Map(unknownView -> ("test", 1L))))

    expectMsgType[NewViewActorRef]
  }

  it should "initialize an external view" in new ViewManagerActorExternalTest {

    val viewWithExt = ViewWithExternalDeps(p("ec0101"), p("2016"), p("11"), p("07"))
    val future = viewManagerActor ? viewWithExt
    val viewE = ExternalView(ExternalShop())

    schemaManagerRouter.expectMsg(CheckOrCreateTables(List(viewWithExt)))
    schemaManagerRouter.reply(SchemaActionSuccess())
    schemaManagerRouter.expectMsg(CheckOrCreateTables(List(viewE)))
    schemaManagerRouter.reply(SchemaActionSuccess())
    schemaManagerRouter.expectMsg(AddPartitions(List(viewWithExt)))
    schemaManagerRouter.reply(TransformationMetadata(Map(viewWithExt -> ("test", 1L))))
    schemaManagerRouter.expectMsg(AddPartitions(List(viewE)))
    schemaManagerRouter.reply(TransformationMetadata(Map(viewE -> ("test", 1L))))

    Await.result(future, 5 seconds)
    future.isCompleted shouldBe true
    future.value.get.isSuccess shouldBe true
    val actorRef = future.value.get.get.asInstanceOf[ActorRef]

    viewManagerActor ! DelegateMessageToView(viewE, MaterializeView())

    expectMsgType[NewViewActorRef]
  }

  it should "throw an exception if external views are not allowed" in new ViewManagerActorTest {

    val viewWithExt = ViewWithExternalDeps(p("ec0101"), p("2016"), p("11"), p("07"))
    an[UnsupportedOperationException] shouldBe thrownBy {
      viewManagerActor.receive(viewWithExt)
    }
  }

  it should "throw an exception if internal views are used as external" in new ViewManagerActorExternalTest {

    val viewWithExt = ViewWithIllegalExternalDeps(p("ec0101"))
    the[UnsupportedOperationException] thrownBy {
      viewManagerActor.receive(viewWithExt)
    } should have message "You are referencing an external view as internal: test.views/Brand/ec0101."
  }

  it should "throw an exception if external views are used as internal" in new ViewManagerActorExternalTest {

    val viewWithExt = ViewWithIllegalInternalDeps(p("ec0101"))
    the[UnsupportedOperationException] thrownBy {
      viewManagerActor.receive(viewWithExt)
    } should have message "You are referencing an internal view as external: test.extviews/Shop/."
  }

  "the check" should "be silenced by the setting" in new ViewManagerActorExternalTest {
    override lazy val settings = TestUtils.createSettings("schedoscope.external-dependencies.enabled=true",
      "schedoscope.external-dependencies.home=[\"dev.test.views\"]",
      "schedoscope.external-dependencies.checks=false")

    val viewWithExt = ViewWithIllegalInternalDeps(p("ec0101"))

    viewManagerActor ! viewWithExt
    schemaManagerRouter.expectMsg(CheckOrCreateTables(List(viewWithExt)))
    schemaManagerRouter.reply(SchemaActionSuccess())
    schemaManagerRouter.expectMsg(CheckOrCreateTables(List(ExternalShop())))
    schemaManagerRouter.reply(SchemaActionSuccess())
    schemaManagerRouter.expectMsg(AddPartitions(List(viewWithExt)))
    schemaManagerRouter.reply(TransformationMetadata(Map(viewWithExt -> ("test", 1L))))
    schemaManagerRouter.expectMsg(AddPartitions(List(ExternalShop())))
    schemaManagerRouter.reply(TransformationMetadata(Map(ExternalShop() -> ("test", 1L))))
  }
}
