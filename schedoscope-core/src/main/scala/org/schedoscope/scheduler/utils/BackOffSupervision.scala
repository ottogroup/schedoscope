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
import scala.concurrent.ExecutionContext

/**
  * A partial implementation of a BackOffStrategy available in
  * Akka >= 2.4.1+, which schedules and stores Exponential duration
  * per actor, and schedules a "tick" activation message send in the
  * system with the purpose of managed actors activation
  *
  * @param managerName     param used for logging purposes
  * @param system          akka system used for scheduling firing messages
  */
class BackOffSupervision(managerName: String,
                         system: ActorSystem) {

  val log = LoggerFactory.getLogger(getClass)

  val actorBackOffWaitTime = HashMap[String, ExponentialBackOff]()

  def scheduleTick(actor: ActorRef, backOffTime: FiniteDuration)(implicit executor: ExecutionContext) {
    system.scheduler.scheduleOnce(backOffTime, actor, "tick")
  }
  def manageActorLifecycle(managedActor: ActorRef, backOffSlotTime: FiniteDuration = null, backOffMinimumDelay: FiniteDuration = null)(implicit executor: ExecutionContext) {
    if(actorBackOffWaitTime.contains(managedActor.path.toStringWithoutAddress)) {

      val newBackOff = actorBackOffWaitTime(managedActor.path.toStringWithoutAddress).nextBackOff

      scheduleTick(managedActor, newBackOff.backOffWaitTime)
      actorBackOffWaitTime.put(managedActor.path.toStringWithoutAddress, newBackOff)
      log.warn(s"$managerName: Set new back-off waiting " +
        s"time to value ${newBackOff.backOffWaitTime} for rebooted actor ${managedActor.path.toStringWithoutAddress}; " +
        s"(retries=${newBackOff.retries}, resets=${newBackOff.resets}, total-retries=${newBackOff.totalRetries})")

    } else {
      managedActor ! "tick"
      val backOff = ExponentialBackOff(backOffSlotTime = backOffSlotTime, constantDelay = backOffMinimumDelay)
      log.debug(s"$managerName: Set initial back-off waiting " +
        s"time to value ${backOff.backOffWaitTime} for booted actor ${managedActor.path.toStringWithoutAddress}; " +
        s"(retries=${backOff.retries}, resets=${backOff.resets}, total-retries=${backOff.totalRetries})")

      actorBackOffWaitTime.put(managedActor.path.toStringWithoutAddress, backOff)
    }
  }

}
