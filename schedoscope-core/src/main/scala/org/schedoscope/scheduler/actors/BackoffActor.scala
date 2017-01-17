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

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}
import akka.event.{Logging, LoggingReceive}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random


trait BackoffActor extends Actor {

  import context._

  val BACKOFFDURATION = 30 seconds
  val log = Logging(system, this)

  def backoffTick() {
    val randomNum = scala.util.Random.nextInt(5) * BACKOFFDURATION
    system.scheduler.scheduleOnce(randomNum, self, "wakeupTick")
    log.info(s"Using manually implemented Backoff Strategy, with a Backoff duration of ${randomNum} seconds")
    become(backingOff)
  }

  def backingOff = LoggingReceive {
    case "wakeupTick" => {
      become(receive)
    }
    case _ => self ! _
  }

}
