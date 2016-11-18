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


class SchedoscopeServiceImplSpec extends TestKit(ActorSystem("schedoscope"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

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
            schemaManagerActor.ref
      )
    )

    Schedoscope.viewManagerActorBuilder = () => viewManagerActor
    val productBrandView = ProductBrand(p("ec0106"), p("2014"), p("01"), p("01"))
    val brandDependency:View = productBrandView.dependencies.head
    val productDependency:View = productBrandView.dependencies(1)

    def initializeView(view: View): ActorRef = {
      val future = viewManagerActor ? view
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(view)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(productDependency)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(CheckOrCreateTables(List(brandDependency)))
      schemaManagerActor.reply(SchemaActionSuccess())
      schemaManagerActor.expectMsg(AddPartitions(List(view)))
      schemaManagerActor.reply(TransformationMetadata(Map(view -> ("test", 1L))))
      schemaManagerActor.expectMsg(AddPartitions(List(productDependency)))
      schemaManagerActor.reply(TransformationMetadata(Map(productDependency -> ("test", 1L))))
      schemaManagerActor.expectMsg(AddPartitions(List(brandDependency)))
      schemaManagerActor.reply(TransformationMetadata(Map(brandDependency -> ("test", 1L))))

      Await.result(future, 5 seconds)
      future.isCompleted shouldBe true
      future.value.get.isSuccess shouldBe true
      future.value.get.get.asInstanceOf[ActorRef]
    }
  }

  "The ViewManagerActor" should "create a new view" in new ViewManagerActorTest {
        initializeView(productBrandView)
  }


}
