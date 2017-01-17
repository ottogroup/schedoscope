package org.schedoscope.scheduler.actors
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.schedoscope.Settings
import org.schedoscope.dsl.Parameter._
import org.schedoscope.dsl.transformations.{FilesystemTransformation, Touch}
import org.schedoscope.scheduler.driver.HiveDriver
import org.schedoscope.scheduler.messages._
import test.views.ProductBrand

import scala.concurrent.duration._

class TransformationManagerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar {

  class ForwardChildActor(to: ActorRef) extends Actor {

    def receive = {
      case x => to.forward(x)
    }
  }

  trait TransformationManagerActorTest {
    lazy val settings = Settings()

    val hiveDriverRouter = TestProbe()
    val mapRedDriverRouter = TestProbe()
    val noopDriverRouter = TestProbe()
    val seqDriverRouter = TestProbe()
    val fsDriverRouter = TestProbe()

    val transformationManagerActor = TestActorRef(new TransformationManagerActor(settings,
      bootstrapDriverActors = false) {
      override def preStart {
        context.actorOf(Props(new ForwardChildActor(hiveDriverRouter.ref)), "hive-router")
        context.actorOf(Props(new ForwardChildActor(mapRedDriverRouter.ref)), "mapreduce-router")
        context.actorOf(Props(new ForwardChildActor(noopDriverRouter.ref)), "noop-router")
        context.actorOf(Props(new ForwardChildActor(seqDriverRouter.ref)), "seq-router")
        context.actorOf(Props(new ForwardChildActor(fsDriverRouter.ref)), "filesystem-router")
      }
    })

    val idleHiveStatus = TransformationStatusResponse("idle", hiveDriverRouter.ref, null, null, null)
    hiveDriverRouter.send(transformationManagerActor, idleHiveStatus)
    val idleMapRedStatus = TransformationStatusResponse("idle", mapRedDriverRouter.ref, null, null, null)
    mapRedDriverRouter.send(transformationManagerActor, idleMapRedStatus)
    val idleNoopStatus = TransformationStatusResponse("idle", noopDriverRouter.ref, null, null, null)
    noopDriverRouter.send(transformationManagerActor, idleNoopStatus)
    val idleSeqStatus = TransformationStatusResponse("idle", seqDriverRouter.ref, null, null, null)
    seqDriverRouter.send(transformationManagerActor, idleSeqStatus)
    val idleFSStatus = TransformationStatusResponse("idle", fsDriverRouter.ref, null, null, null)
    fsDriverRouter.send(transformationManagerActor, idleFSStatus)

    val baseTransformationStatusList = TransformationStatusListResponse(
      List(idleSeqStatus, idleNoopStatus, idleFSStatus, idleMapRedStatus, idleHiveStatus ))
  }

  it should "forward transformations to the correct DriverManager based on incoming View" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      val cmd = DriverCommand(TransformView(testView.transformation(), testView), self)
      transformationManagerActor ! testView
      hiveDriverRouter.expectMsg(cmd)

    }

  it should "forward transformations to the correct DriverManager based on incoming Transformation" in
    new TransformationManagerActorTest {
      val filesystemTransformation = new FilesystemTransformation
      val cmd = DriverCommand(filesystemTransformation, self)
      //val command = DriverCommand(cmd, self)
      transformationManagerActor ! filesystemTransformation
      fsDriverRouter.expectMsg(cmd)
      hiveDriverRouter.expectNoMsg(3 seconds)

    }

  it should "multicast deployCommand to routers" in
    new TransformationManagerActorTest {
      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      val cmd = DriverCommand(DeployCommand(), self)
      //val command = DriverCommand(cmd, self)
      transformationManagerActor ! DeployCommand()
      hiveDriverRouter.expectMsg(cmd)
      mapRedDriverRouter.expectMsg(cmd)
      noopDriverRouter.expectMsg(cmd)
      seqDriverRouter.expectMsg(cmd)
      fsDriverRouter.expectMsg(cmd)
    }

  it should "return the status of transformations (no running transformations)" in
    new TransformationManagerActorTest {
      transformationManagerActor ! GetTransformations()
      expectMsg(baseTransformationStatusList)
    }

  it should "return the status of transformations" in
    new TransformationManagerActorTest {

      val testView = ProductBrand(p("1"), p("2"), p("3"), p("4"))
      val command = DriverCommand(TransformView(testView.transformation(), testView), self)
      transformationManagerActor ! testView
      hiveDriverRouter.expectMsg(command)

      val busyHiveStatus = TransformationStatusResponse("running", hiveDriverRouter.ref,
        HiveDriver(settings.getDriverSettings("hive")), null, null)
      hiveDriverRouter.send(transformationManagerActor, busyHiveStatus)

      transformationManagerActor ! GetTransformations()

      val newTransformationStatusList = TransformationStatusListResponse(
        List(idleFSStatus, busyHiveStatus, idleSeqStatus, idleNoopStatus, idleMapRedStatus ))

      expectMsg(newTransformationStatusList)

    }

  /*
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
    */

}
