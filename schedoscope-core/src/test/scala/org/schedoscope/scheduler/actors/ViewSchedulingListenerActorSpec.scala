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
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import org.joda.time.LocalDateTime
import org.schedoscope.dsl.Parameter._
import org.schedoscope.scheduler.listeners.{RetryableViewSchedulingListenerException, ViewSchedulingListenerException}
import org.schedoscope.scheduler.messages.{RegisterFailedListener, ViewSchedulingMonitoringEvent}
import org.schedoscope.scheduler.states._
import test.views.Brand

class ViewSchedulingListenerActorSpec extends TestKit(ActorSystem("schedoscope"))
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  val TIMEOUT = 5 seconds

  "A viewSchedulingListenerActor" should "execute listener handler methods" in {
    val viewSchedulingListenerManagerActor = TestProbe()
    val handlerClassName = "org.schedoscope.test.TestViewListener"
    val viewSchedulingListenerActor = TestActorRef(ViewSchedulingListenerActor.props(
      handlerClassName, viewSchedulingListenerManagerActor.ref))

    val view = Brand(p("ec01"))
    val prevState1 = CreatedByViewManager(view)

    // confirm listener method is being executed correctly
    intercept[RetryableViewSchedulingListenerException] {
      viewSchedulingListenerActor.receive(
        ViewSchedulingMonitoringEvent(prevState1, prevState1, Set(Transform(view)), new LocalDateTime()))
    }
    // since at it, confirm that listener actor handles retryable exceptions
    // and tries to cache in viewSchedulingListenerManagerActor as receiver of
    // latest events
    viewSchedulingListenerManagerActor.expectMsg(RegisterFailedListener(handlerClassName))

    val newState1 = Failed(view)
    // confirm listener method is being executed correctly
    intercept[ViewSchedulingListenerException] {
      viewSchedulingListenerActor.receive(
        ViewSchedulingMonitoringEvent(prevState1, newState1, Set(Transform(view)), new LocalDateTime()))
    }
    // Confirm that listener actor does not register for receiving latest events!
    viewSchedulingListenerManagerActor.expectNoMsg(TIMEOUT)
  }


}
