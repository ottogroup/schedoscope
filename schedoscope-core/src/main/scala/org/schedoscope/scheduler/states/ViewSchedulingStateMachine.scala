package org.schedoscope.scheduler.states

import org.schedoscope.dsl.transformations.NoOp
import org.schedoscope.dsl.View
import java.util.Date
import scala.language.implicitConversions

/**
 * Representation of state transition results from view scheduling state machines.
 */
sealed abstract class ViewSchedulingStateTransition

/**
 * A view has transitioned to a new scheduling state. There is a set of necessary state transition actions associated with the transition.
 */
case class NewState(
  newState: ViewSchedulingState,
  actions: Set[ViewSchedulingStateTransitionAction]) extends ViewSchedulingStateTransition

/**
 * A view's state hasn't changed.
 */
case class NoStateChange() extends ViewSchedulingStateTransition

/**
 * Trait capturing the common interface of view scheduling state machines. The interface can be implemented differently
 * for different transformation types. Essentially, a view's scheduling state changes (or not) upon processing of commands.
 * These are represented by the various methods of the interface.
 */
trait ViewSchedulingStateMachine {

  /**
   * Given the view's current view scheduling state apply a materialize command. This issuer of the command is passed along as an interested listener in the outcome.
   *
   * The outcome is influenced by whether a _SUCCESS flag exists in the view's fullPath and the current time.
   */
  def materialize(currentState: ViewSchedulingState, issuer: ViewSchedulingStateListener, successFlagExists: => Boolean, currentTime: Long = new Date().getTime): ViewSchedulingStateTransition
}

object ViewSchedulingStateMachine {

  /**
   * Implicit factory of the view scheduling state machine appropriate for a view's transformation type.
   */
  implicit def schedulingStateMachineForView(view: View) = view.transformation() match {
    case NoOp() => new NoOpViewSchedulingStateMachine
    case _      => throw new RuntimeException("Unschedulable Transformation Type")
  }
}