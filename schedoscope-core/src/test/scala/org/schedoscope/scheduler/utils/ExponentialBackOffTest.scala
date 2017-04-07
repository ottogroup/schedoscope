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

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class ExponentialBackOffTest extends FlatSpec with Matchers {

  val backOffSlotTime = 5 seconds


  it should "generate a new back-off time equal to zero or X times backOffSlotTime" in {
    val backOff = ExponentialBackOff(backOffSlotTime = backOffSlotTime, constantDelay = Duration.Zero).nextBackOff
    (backOff.backOffWaitTime.equals(Duration.Zero) || backOff.backOffWaitTime.equals(backOffSlotTime)) shouldBe true
    //second slot
    val secondSlotBackOff = backOff.nextBackOff
    (secondSlotBackOff.backOffWaitTime.equals(Duration.Zero) || secondSlotBackOff.backOffWaitTime.equals(backOffSlotTime)
      || secondSlotBackOff.backOffWaitTime.equals(3 * backOffSlotTime)) shouldBe true

  }

  it should "generate a new back-off time bigger or equal than minimum constant delay" in {
    val constantDelay = 1 seconds
    val backOff = ExponentialBackOff(backOffSlotTime = backOffSlotTime, constantDelay = constantDelay)
      .nextBackOff
      .backOffWaitTime
    backOff.gteq(constantDelay) shouldBe true
  }

}
