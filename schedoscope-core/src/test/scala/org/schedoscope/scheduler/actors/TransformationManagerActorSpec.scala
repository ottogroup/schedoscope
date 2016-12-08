package org.schedoscope.scheduler.actors
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.Touch
import org.schedoscope.scheduler.driver.HiveDriver
import org.schedoscope.scheduler.messages._
import test.views.ProductBrand

class TransformationManagerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  trait TransformationManagerActorTest {
    lazy val settings = Settings()

    val transformationManagerActor = TestActorRef(TransformationManagerActor.props(settings,
      bootstrapDriverActors = false))

    val hiveDriverActor = TestProbe()
  }

  "the TransformationManagerActor" should "enqueue a transformation" in new TransformationManagerActorTest {
    val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
    transformationManagerActor ! testView
    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(),
      "mapreduce" -> List(),
      "noop" -> List(),
      "hive" -> List(TransformView(testView.transformation(), testView)),
      "seq" -> List())))
  }

  it should "enqueue a deploy command" in new TransformationManagerActorTest {
    transformationManagerActor ! DeployCommand()
    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(DeployCommand()),
      "mapreduce" -> List(DeployCommand()),
      "noop" -> List(DeployCommand()),
      "hive" -> List(DeployCommand()),
      "seq" -> List(DeployCommand()))))
  }

  it should "enqueue a filesystem transformation" in new TransformationManagerActorTest {
    transformationManagerActor ! Touch("test")
    transformationManagerActor ! GetQueues()
    expectMsg(QueueStatusListResponse(Map("filesystem-0" -> List(Touch("test")),
      "mapreduce" -> List(),
      "noop" -> List(),
      "hive" -> List(),
      "seq" -> List())))
  }

  it should "dequeue a transformation" in new TransformationManagerActorTest {
    val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
    transformationManagerActor ! testView
    val command = DriverCommand(TransformView(testView.transformation(), testView), self)
    transformationManagerActor ! PollCommand("hive")
    expectMsg(command)
  }

  it should "dequeue a deploy command" in new TransformationManagerActorTest {
    transformationManagerActor ! DeployCommand()
    val command = DriverCommand(DeployCommand(), self)
    transformationManagerActor ! PollCommand("hive")
    expectMsg(command)
  }

  it should "dequeue a filesystem transformation" in new TransformationManagerActorTest {
    transformationManagerActor ! Touch("test")
    val command = DriverCommand(Touch("test"), self)
    transformationManagerActor ! PollCommand("filesystem-0")
    expectMsg(command)
  }

  it should "return the status of transformations (no running transformations)" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      transformationManagerActor ! testView
      transformationManagerActor ! GetTransformations()
      expectMsg(TransformationStatusListResponse(List()))
    }

  it should "return the status of transformations" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      val command = DriverCommand(TransformView(testView.transformation(), testView), self)
      transformationManagerActor ! testView
      transformationManagerActor ! PollCommand("hive")
      expectMsg(command)
      transformationManagerActor ! GetTransformations()
      expectMsg(TransformationStatusListResponse(List()))
      val transformationStatusResponse = TransformationStatusResponse("running", hiveDriverActor.ref, HiveDriver(settings.getDriverSettings("hive")), null, null)
      transformationManagerActor ! transformationStatusResponse
      transformationManagerActor ! GetTransformations()
      expectMsg(TransformationStatusListResponse(List(transformationStatusResponse)))
    }


}
