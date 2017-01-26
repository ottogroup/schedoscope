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

import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.util.Random

/**
  * Implementing Exponential Back-off Algorithm
  * https://en.wikipedia.org/wiki/Exponential_backoff
  *
  * @param backOffSlotTime    the time value of a slot; in wikipedia network example, this would be the 51.2 microseconds
  * @param backOffSlot        counter for exponential growth; in wikipedia example, this would be the collisions count
  * @param backOffWaitTime    the actual waiting time for a given iteration
  * @param constantDelay     add, optionally, variant to algorithm to allow guarantee of minimum delay > 0
  * @param ceiling            optionally define an upper limit for the max number of collisions multiplier
  * @param resetOnCeiling     reset algorithm collision count if ceiling reached
  * @param retries            counter for number of retries/collisions occurred
  * @param resets             counter for number of times ceiling was reached
  * @param totalRetries       counter of absolute total retries, independently of resets
  */
case class ExponentialBackOff(backOffSlotTime: FiniteDuration,
                              backOffSlot: Int = 1,
                              backOffWaitTime: FiniteDuration = Duration.Zero,
                              constantDelay: FiniteDuration = Duration.Zero,
                              ceiling: Int = 10,
                              resetOnCeiling: Boolean = false,
                              retries: Int = 0,
                              resets: Int = 0,
                              totalRetries: Long = 0) {

  private def updateTime = backOffSlotTime * expectedBackOff(backOffSlot) + constantDelay

  private def expectedBackOff(backOffSlot: Int) = {
    val rand = new Random().nextInt(backOffSlot + 1)
    math.round(math.pow(2, rand)-1)
  }

  def nextBackOff: ExponentialBackOff = {
    if(backOffSlot >= ceiling && resetOnCeiling)
      // reset
      copy(backOffSlot = 1,
        backOffWaitTime = Duration.Zero,
        resets = resets +1,
        retries = 0,
        totalRetries = totalRetries +1)
    else {
      val newBackOffSlot = if(backOffSlot >= ceiling) ceiling else backOffSlot + 1
      // increase 1 collision
      copy(backOffSlot = newBackOffSlot,
        backOffWaitTime = updateTime,
        retries = retries +1,
        totalRetries = totalRetries +1)
    }
  }
}
