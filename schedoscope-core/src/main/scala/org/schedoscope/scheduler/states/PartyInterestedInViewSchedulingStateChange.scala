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
package org.schedoscope.scheduler.states

import akka.actor.ActorRef
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.actors.ViewManagerActor

import scala.language.implicitConversions

/**
  * A view scheduling state machine might want to know who needs to be informed about a state transition.
  * These listeners may be of different type, which are subsumed and wrapped by this class.
  *
  * For informing, one needs to provide an adequate message sending ! operator
  */
sealed trait PartyInterestedInViewSchedulingStateChange{
  val view: Option[View]
}

/**
  * A view depending on a given view with a state change.
  */
case class DependentView(dependentView: View) extends PartyInterestedInViewSchedulingStateChange{
  val view = Some(dependentView)
}

/**
  * A generic Akka actor interested in a state change
  */
case class AkkaActor(view: Option[View], actorRef: ActorRef) extends PartyInterestedInViewSchedulingStateChange

object PartyInterestedInViewSchedulingStateChange {
  /**
    * Implicit conversion of view to party type
    */
  implicit def toParty(view: View): DependentView = DependentView(view)

  /**
    * Implicit conversion of view to party type
    */
  implicit def toParty(actorRef: ActorRef): AkkaActor = AkkaActor(None, actorRef)
}