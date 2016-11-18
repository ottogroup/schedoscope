package org.schedoscope.scheduler.service

import akka.actor.{ActorRef, ActorSystem}
import akka.actor.{Actor, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.schedoscope.scheduler.actors.ViewManagerActor
//import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages._
import org.schedoscope.{Schedoscope, Settings}
import test.views.{Brand, ProductBrand}

import scala.concurrent.Await
import scala.concurrent.duration._
import org.schedoscope.dsl.Parameter


class SchedoscopeServiceImplSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  // dummy data
  val year:Parameter[String] = p("2014")
  val month:Parameter[String] = p("01")
  val day:Parameter[String] = p("01")
  val shopCode01:Parameter[String] = p("EC01")
  val shopCode02:Parameter[String] = p("EC02")

  val productBrandView01 = ProductBrand(shopCode01, year, month, day)
  val brandDependency01:View = productBrandView01.dependencies.head
  val productDependency01:View = productBrandView01.dependencies(1)

  val productBrandView02 = ProductBrand(shopCode02, year, month, day)
  val brandDependency02:View = productBrandView02.dependencies.head
  val productDependency02:View = productBrandView02.dependencies(1)

  trait ViewManagerActorTest {

    implicit val timeout = Timeout(5 seconds)

    val schemaManagerActor = TestProbe()
    val actionsManagerActor = TestProbe()
    val transformationManagerActor = TestProbe()
    val config = ConfigFactory.load()

    Schedoscope.actorSystemBuilder = () => system

    val viewManagerActor = TestActorRef(
      ViewManagerActor.props(
            Schedoscope.settings,
            actionsManagerActor.ref,
            schemaManagerActor.ref,
            schemaManagerActor.ref)
    )

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor

    def initializeView(view: View, brandDependency:View, productDependency:View): ActorRef = {
      val future = viewManagerActor ? view

      schemaManagerActor.expectMsg(CheckOrCreateTables(List(brandDependency)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(productDependency)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(AddPartitions(List(brandDependency)))
      schemaManagerActor.reply(TransformationMetadata(Map(brandDependency -> ("test", 1L))))
      schemaManagerActor.expectMsg(AddPartitions(List(view)))
      schemaManagerActor.reply(TransformationMetadata(Map(view -> ("test", 1L))))
      schemaManagerActor.expectMsg(AddPartitions(List(productDependency)))
      schemaManagerActor.reply(TransformationMetadata(Map(productDependency -> ("test", 1L))))

      Await.result(future, 5 seconds)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      println(future.value.get)
      println(future.value.get.get)
      future.value.get.get.asInstanceOf[ActorRef]
    }
  }

  trait SchedoscopeServiceTest extends ViewManagerActorTest {

    Schedoscope.actorSystemBuilder = () => system

    lazy val service = new SchedoscopeServiceImpl(system,
      Schedoscope.settings,
      viewManagerActor,
      transformationManagerActor.ref)

  }


  "The ViewManagerActor" should "create a new view" in new SchedoscopeServiceTest {
        initializeView(productBrandView01, brandDependency01, productDependency01)
  }

  "The SchedoscopeService" should "create a get details about all views it knows about" in new SchedoscopeServiceTest {
    //val (brandViewActor, prodBrandViewActor, prodViewActor) = initializeView(productBrandView01, brandDependency01, productDependency01)
    val initStatus = "receive"

    val brandViewActor = initializeView(productBrandView01, brandDependency01, productDependency01)


    val prodBrandStatusResponse = ViewStatusResponse(initStatus, productBrandView01, brandViewActor)
    val brandStatusResponse = ViewStatusResponse(initStatus, productBrandView01, brandViewActor)
    val prodStatusResponse = ViewStatusResponse(initStatus, productBrandView01, prodViewActor)


    val viewUrlPath = Some("views")
    val statusParam = Some("")
    val filterParam = Some("")
    val dependenciesParam = Some(true)
    val overviewParam = Some(true)
    val allParam = Some(true)

    val res = service.views(viewUrlPath, statusParam, filterParam, dependenciesParam, overviewParam, allParam)
    Await.result(res, 5 seconds)

    /*
    val viewStatusProdBrand = ViewStatus()
    val viewStatusBrand = ViewStatus()
    val viewStatusProd = ViewStatus()
    val viewStatusOverviewExpected:Map[String, Int] = Map()
    val expected = ViewStatusList(
      viewStatusOverviewExpected,
      List(viewStatusProdBrand, viewStatusBrand, viewStatusProd))
    */
  }


}