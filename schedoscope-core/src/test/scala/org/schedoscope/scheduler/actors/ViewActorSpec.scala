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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.ExternalView
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.scheduler.driver.{DriverRunHandle, DriverRunSucceeded, HiveDriver}
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states.CreatedByViewManager
import test.views.{ProductBrand, ViewWithExternalDeps}


class ViewActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  trait ViewActorTest {
    val viewManagerActor = TestProbe()

    val transformationManagerActor = TestProbe()
    val schemaManagerRouter = TestProbe()
    val viewSchedulingListenerManagerActor = TestProbe()

    val brandViewActor = TestProbe()
    val productViewActor = TestProbe()

    val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val brandDependency = view.dependencies.head
    val productDependency = view.dependencies(1)

    val viewActor = TestActorRef(ViewActor.props(
      CreatedByViewManager(view),
      Settings(),
      Map(brandDependency -> brandViewActor.ref,
        productDependency -> productViewActor.ref),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))
  }

  "The ViewActor" should "send materialize to deps" in new ViewActorTest {
    viewActor ! MaterializeView()
    brandViewActor.expectMsg(MaterializeView())
    productViewActor.expectMsg(MaterializeView())
  }

  it should "add new dependencies" in new ViewActorTest {
    val emptyDepsViewActor = system.actorOf(ViewActor.props(
      CreatedByViewManager(view),
      Settings(),
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))

    emptyDepsViewActor ! NewViewActorRef(brandDependency, brandViewActor.ref)
    emptyDepsViewActor ! NewViewActorRef(productDependency, productViewActor.ref)

    emptyDepsViewActor ! MaterializeView()

    brandViewActor.expectMsg(MaterializeView())
    productViewActor.expectMsg(MaterializeView())
  }

  it should "ask if he doesn't know another ViewActor" in new ViewActorTest {
    val emptyDepsViewActor = system.actorOf(ViewActor.props(
      CreatedByViewManager(view),
      Settings(),
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))

    emptyDepsViewActor ! MaterializeView()

    viewManagerActor.expectMsg(DelegateMessageToView(brandDependency, MaterializeView()))
    viewManagerActor.expectMsg(DelegateMessageToView(productDependency, MaterializeView()))
  }

  it should "send a message to the transformation actor when ready to transform" in new ViewActorTest {
    viewActor ! MaterializeView()
    brandViewActor.expectMsg(MaterializeView())
    brandViewActor.reply(ViewMaterialized(brandDependency, incomplete = false, 1L, errors = false))
    productViewActor.expectMsg(MaterializeView())
    productViewActor.reply(ViewMaterialized(productDependency, incomplete = false, 1L, errors = false))

    transformationManagerActor.expectMsg(view)
  }

  it should "materialize a view successfully" in new ViewActorTest {

    viewActor ! MaterializeView()
    brandViewActor.expectMsg(MaterializeView())
    brandViewActor.reply(ViewMaterialized(brandDependency, incomplete = false, 1L, errors = false))
    productViewActor.expectMsg(MaterializeView())
    productViewActor.reply(ViewMaterialized(productDependency, incomplete = false, 1L, errors = false))

    transformationManagerActor.expectMsg(view)
    val success = TransformationSuccess(mock[DriverRunHandle[HiveTransformation]], mock[DriverRunSucceeded[HiveTransformation]], true)
    transformationManagerActor.reply(success)

    expectMsgType[ViewMaterialized]
  }

  it should "materialize an external view" in new ViewActorTest {

    val viewWithExt = ViewWithExternalDeps(p("ec0101"), p("2016"), p("11"), p("07"))
    val extView = viewWithExt.dependencies.head
    val extActor = TestProbe()
    val actorWithExt = system.actorOf(ViewActor.props(
      CreatedByViewManager(viewWithExt),
      Settings(),
      Map(extView -> extActor.ref),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))

    actorWithExt ! MaterializeView()
    extActor.expectMsg(MaterializeExternalView())
    extActor.reply(ViewMaterialized(extView, incomplete = false, 1L, errors = false))
    transformationManagerActor.expectMsg(viewWithExt)
    val success = TransformationSuccess(mock[DriverRunHandle[HiveTransformation]], mock[DriverRunSucceeded[HiveTransformation]], true)
    transformationManagerActor.reply(success)

    expectMsgType[ViewMaterialized]
  }

  "A external view" should "reload it's state and ignore it's deps" in new ViewActorTest {
    val extView = ExternalView(ProductBrand(p("ec0101"), p("2016"), p("11"), p("07")))

    val extActor = system.actorOf(ViewActor.props(
      CreatedByViewManager(extView),
      Settings(),
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))

    extActor ! MaterializeExternalView()

    schemaManagerRouter.expectMsg(GetMetaDataForMaterialize(extView,
      MaterializeViewMode.DEFAULT,
      self))

    schemaManagerRouter.reply(MetaDataForMaterialize((extView, ("checksum", 1L)),
      MaterializeViewMode.DEFAULT,
      self))

    transformationManagerActor.expectMsg(extView)
    val success = TransformationSuccess(mock[DriverRunHandle[HiveTransformation]], mock[DriverRunSucceeded[HiveTransformation]], true)
    transformationManagerActor.reply(success)

    expectMsgType[ViewMaterialized]

  }

  "A view" should "should reload it's state and ignore it's deps when called view external materialize" in new ViewActorTest {
    val viewNE = ProductBrand(p("ec0101"), p("2016"), p("11"), p("07"))
    val viewE = ExternalView(ProductBrand(p("ec0101"), p("2016"), p("11"), p("07")))

    val extActor = system.actorOf(ViewActor.props(
      CreatedByViewManager(viewE),
      Settings(),
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref,
      viewSchedulingListenerManagerActor.ref))

    extActor ! MaterializeExternalView()

    schemaManagerRouter.expectMsg(GetMetaDataForMaterialize(viewE,
      MaterializeViewMode.DEFAULT,
      self))

    schemaManagerRouter.reply(MetaDataForMaterialize((viewE, ("checksum", 1L)),
      MaterializeViewMode.DEFAULT,
      self))

    transformationManagerActor.expectMsg(viewE)
    val success = TransformationSuccess(mock[DriverRunHandle[HiveTransformation]], mock[DriverRunSucceeded[HiveTransformation]], true)
    transformationManagerActor.reply(success)

    expectMsgType[ViewMaterialized]

  }

}
