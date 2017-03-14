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
package org.schedoscope.scheduler.utils

import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

/**
  * A partial implementation of a BackOffStrategy available in
  * Akka >= 2.4.1+, which stores Exponential duration
  * per actor, and returns a duration duration time.
  * The returned duration time can then be used by an actor
  * to schedule a "tick" activation message send in the
  * system (with the purpose of managed actors activation)
  *
  * @param managerName param used for logging purposes
  * @param system      akka system used for scheduling firing messages
  */
class BackOffSupervision(managerName: String,
                         system: ActorSystem) {

  val log = LoggerFactory.getLogger(getClass)
  val actorBackOffWaitTime = HashMap[String, ExponentialBackOff]()

  /**
    * Stores a managedActor ExponentialBackOff object, which
    * itself contains a current waiting time value (finite duration)
    *
    * @param managedActor        actor that requires backoff supervision
    * @param backOffSlotTime     duration of the backoff slot
    * @param backOffMinimumDelay minimum base delay configured for actor
    * @return the current waiting time
    */
  def manageActorLifecycle(managedActor: ActorRef, backOffSlotTime: FiniteDuration = null, backOffMinimumDelay: FiniteDuration = null): FiniteDuration = {
    val managedActorName = managedActor.path.toStringWithoutAddress

    if (actorBackOffWaitTime.contains(managedActorName)) {
      val newBackOff = actorBackOffWaitTime(managedActorName).nextBackOff
      actorBackOffWaitTime.put(managedActorName, newBackOff)
      log.warn(s"$managerName: Set new back-off waiting " +
        s"time to value ${newBackOff.backOffWaitTime} for rebooted actor ${managedActorName}; " +
        s"(retries=${newBackOff.retries}, resets=${newBackOff.resets}, total-retries=${newBackOff.totalRetries})")

      //schedule tick response based on backoff
      newBackOff.backOffWaitTime
    } else {
      val backOff = ExponentialBackOff(backOffSlotTime = backOffSlotTime, constantDelay = backOffMinimumDelay)
      log.debug(s"$managerName: Set initial back-off waiting " +
        s"time to value ${backOff.backOffWaitTime} for booted actor ${managedActorName}; " +
        s"(retries=${backOff.retries}, resets=${backOff.resets}, total-retries=${backOff.totalRetries})")
      actorBackOffWaitTime.put(managedActorName, backOff)

      //schedule immediate tick response
      0 millis
    }
  }

}
