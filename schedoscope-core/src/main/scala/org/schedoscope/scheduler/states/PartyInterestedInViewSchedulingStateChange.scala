package org.schedoscope.scheduler.states

import scala.language.implicitConversions
import org.schedoscope.dsl.View
import akka.actor.ActorRef

/**
 * A view scheduling state machine might want to know who needs to be informed about a state transition.
 * These listeners may be of different type, which are subsumed and wrapped by this class.
 */
sealed abstract class PartyInterestedInViewSchedulingStateChange

/**
 * A view depending on a given view with a state change.
 */
case class DependentView(view: View) extends PartyInterestedInViewSchedulingStateChange

/**
 * A generic Akka actor interested in a state change
 */
case class AkkaActor(actorRef: ActorRef) extends PartyInterestedInViewSchedulingStateChange

object PartyInterestedInViewSchedulingStateChange {
  /**
   * Implicit conversion of view to party type
   */
  implicit def toParty(view: View) = DependentView(view)
  
  /**
   * Implicit conversion of view to party type
   */
  implicit def toParty(actorRef: ActorRef) = AkkaActor(actorRef)
}