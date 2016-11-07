package org.schedoscope.scheduler.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.scheduler.messages._
import org.schedoscope.scheduler.states.CreatedByViewManager
import org.schedoscope.{Schedoscope, Settings}
import test.views.{ProductBrand, ViewWithExternalDeps}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.schedoscope.dsl.ExternalView


object ForwardActor {
  def props(to: ActorRef) = Props(new ForwardActor(to))
}

class ForwardActor(to: ActorRef) extends Actor {
  override def receive = {
    case x =>
      println(x)
      to forward x
  }
}

class ParentActor


class ViewActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  def mockPath(name: String, probe: TestProbe)(implicit system: ActorSystem): Unit =
    system.actorOf(ForwardActor.props(probe.ref), name)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  trait ViewActorTest {
    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestProbe()

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor.ref

    val transformationManagerActor = TestProbe()
    val schemaManagerRouter = TestProbe()
    val brandViewActor = TestProbe()
    val productViewActor = TestProbe()
//    mockPath("test.views:Brand:ec0106", brandViewActor)
//    mockPath("test.views:Product:ec0106:2014:01:01:20140101", productViewActor)
    val fileSystem = mock[FileSystem]



    val view = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val brandDependency = view.dependencies.head
    val productDependency = view.dependencies(1)

    val viewActor = TestActorRef(ViewActor.props(
      CreatedByViewManager(view),
      Settings(),
      fileSystem,
      Map(brandDependency -> brandViewActor.ref,
        productDependency -> productViewActor.ref),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref))
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
      fileSystem,
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref))

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
      fileSystem,
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref))

    emptyDepsViewActor ! MaterializeView()

    viewManagerActor.expectMsg(DelegateMessageToView(brandDependency, MaterializeView()))
    viewManagerActor.expectMsg(DelegateMessageToView(productDependency, MaterializeView()))
  }

  it should "send a message to the transformation actor when ready to transform" in new ViewActorTest {
    viewActor ! MaterializeView()
    brandViewActor.expectMsg(MaterializeView())
    brandViewActor.reply(ViewMaterialized(brandDependency,incomplete = false, 1L, errors = false))
    productViewActor.expectMsg(MaterializeView())
    productViewActor.reply(ViewMaterialized(productDependency,incomplete = false, 1L, errors = false))

    transformationManagerActor.expectMsg(view)
  }

  it should "materialize a view successfully" in new ViewActorTest {

    when(fileSystem.listStatus(any(classOf[Path]),any(classOf[PathFilter])))
      .thenReturn(List(new FileStatus(1L,false,1,12L,0L,new Path("test"))).toArray)

    viewActor ! MaterializeView()
    brandViewActor.expectMsg(MaterializeView())
    brandViewActor.reply(ViewMaterialized(brandDependency,incomplete = false, 1L, errors = false))
    productViewActor.expectMsg(MaterializeView())
    productViewActor.reply(ViewMaterialized(productDependency,incomplete = false, 1L, errors = false))

    transformationManagerActor.expectMsg(view)
    val success = mock[TransformationSuccess[HiveTransformation]]
    transformationManagerActor.reply(success)

    expectMsgType[ViewMaterialized]
  }

  it should "materialize an external view" in new ViewActorTest {

    when(fileSystem.listStatus(any(classOf[Path]),any(classOf[PathFilter])))
      .thenReturn(List(new FileStatus(1L,false,1,12L,0L,new Path("test"))).toArray)

    val viewWithExt = ViewWithExternalDeps(p("ec0101"),p("2016"),p("11"),p("07"))
    val extView = viewWithExt.dependencies.head
    val extActor = TestProbe()
    val actorWithExt = system.actorOf(ViewActor.props(
      CreatedByViewManager(viewWithExt),
      Settings(),
      fileSystem,
      Map(extView -> extActor.ref),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref))

    actorWithExt ! MaterializeView()
    extActor.expectMsg(ReloadStateAndMaterializeView())
    extActor.reply(ViewMaterialized(extView, incomplete = false, 1L, errors = false))
    transformationManagerActor.expectMsg(viewWithExt)
    val success = mock[TransformationSuccess[HiveTransformation]]
    transformationManagerActor.reply(success)

    expectMsgType[ViewMaterialized]
  }

  "A external view" should "reload it's state and ignore it's deps" in new ViewActorTest {
    val extView = ExternalView(ProductBrand(p("ec0101"),p("2016"),p("11"),p("07")))

    val extActor = system.actorOf(ViewActor.props(
      CreatedByViewManager(extView),
      Settings(),
      fileSystem,
      Map(),
      viewManagerActor.ref,
      transformationManagerActor.ref,
      schemaManagerRouter.ref))

    extActor ! ReloadStateAndMaterializeView()

    schemaManagerRouter.expectMsg(GetMetaDataForMaterialize(extView,
      MaterializeViewMode.DEFAULT,
      self))

    schemaManagerRouter.reply(MetaDataForMaterialize((extView,("checksum",1L)),
      MaterializeViewMode.DEFAULT,
      self))

    expectMsgType[ViewMaterialized]

  }

}
