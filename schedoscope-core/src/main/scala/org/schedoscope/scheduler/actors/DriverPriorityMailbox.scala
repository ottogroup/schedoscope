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
import akka.dispatch.PriorityGenerator
import akka.dispatch.UnboundedPriorityMailbox
import com.typesafe.config.Config

/**
  * A driver actor requires prioritization of tick messages,
  * in order to guarantee successful progress tracking, and
  * to new Mailbox successful moving forward to Mailbox queued
  * transformation tasks
  *
  */
class DriverPriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedPriorityMailbox (
    // Note: lower prio means more important
    PriorityGenerator {
      // prioritize tick for progress tracking
      case "tick" => 0
      // else default
      case _ => 1
    })
