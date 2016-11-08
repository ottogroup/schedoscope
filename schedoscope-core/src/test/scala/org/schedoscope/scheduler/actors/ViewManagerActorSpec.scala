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
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages._
import org.schedoscope.{Schedoscope, Settings}
import test.views.{Brand, ProductBrand}

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
    val schemaManagerRouter = TestProbe()
    val actionsManagerActor = TestProbe()

    val viewManagerActor = TestActorRef(ViewManagerActor.props(
      Settings(),
      actionsManagerActor.ref,
      schemaManagerRouter.ref))
    val transformationManagerActor = TestProbe()


    Schedoscope.actorSystemBuilder = () => system
    Schedoscope.viewManagerActorBuilder = () => viewManagerActor
    val brandViewActor = TestProbe()
    val productViewActor = TestProbe()
    val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val brandDependency = view.dependencies.head
    val productDependency = view.dependencies(1)

    def initializeView(view: View): ActorRef = {
      implicit val timeout = Timeout(5 seconds)
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


  "The ViewManagerActor" should "create a new view" in new ViewManagerActorTest {
    initializeView(view)
  }

  it should "delegate a message to a view" in new ViewManagerActorTest {
    val actorRef = initializeView(view)

    viewManagerActor ! DelegateMessageToView(view, "test")

    expectMsg(NewViewActorRef(view, actorRef))
  }

//  it should "delegate a message to a unknown view" in new ViewManagerActorTest {
//    val unknownView = Brand(p("ec0101"))
//    val actorRef = initializeView(view)
//    viewManagerActor ! DelegateMessageToView(unknownView, "test")
//
//    //if ViewManager does not know view it will start to communicate with
//    //the SchemaManager
//    schemaManagerRouter.expectMsg(CheckOrCreateTables(List(unknownView)))
//    schemaManagerRouter.reply(SchemaActionSuccess())
//    schemaManagerRouter.expectMsg(AddPartitions(List(unknownView)))
//    schemaManagerRouter.reply(TransformationMetadata(Map(unknownView -> ("test", 1L))))
//
//    expectMsgType[NewViewActorRef]
//  }


}
