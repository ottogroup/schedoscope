package org.schedoscope.scheduler.states

import org.schedoscope.dsl.transformations.NoOp
import org.schedoscope.dsl.View
import java.util.Date
import scala.language.implicitConversions
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import org.schedoscope.Settings
import org.schedoscope.Schedoscope

/**
 * The result of applying a view scheduling state machine function. Contains the current state (which may be unchanged)
 * and a set of actions that need to be applied because of the result.
 */
case class ResultingViewSchedulingState(
  currentState: ViewSchedulingState,
  actions: Set[ViewSchedulingAction])

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
  def materialize(currentState: ViewSchedulingState, issuer: PartyInterestedInViewSchedulingStateChange, successFlagExists: => Boolean, materializationMode: MaterializeViewMode = DEFAULT, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
   * Given the view's current view scheduling state apply an invalidate command. This issuer of the command is passed along as an interested listener in the outcome.
   */
  def invalidate(currentState: ViewSchedulingState, issuer: PartyInterestedInViewSchedulingStateChange): ResultingViewSchedulingState

  /**
   * Apply a NoData report of a dependency to the current scheduling state of a waiting view.
   *
   * The outcome may be influenced by whether a _SUCCESS flag exists in the view's fullPath and the current time.
   */
  def noDataAvailable(currentState: Waiting, reportingDependency: View, successFlagExists: => Boolean, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
   * Apply a Failed report of a dependency to the current scheduling state of a waiting view.
   *
   * The outcome may be influenced by whether a _SUCCESS flag exists in the view's fullPath and the current time.
   */
  def failed(currentState: Waiting, reportingDependency: View, successFlagExists: => Boolean, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
   * Apply a Materialized report of a dependency to the current scheduling state of a waiting view.
   *
   * The outcome may be influenced by whether a _SUCCESS flag exists in the view's fullPath and the current time.
   */
  def materialized(currentState: Waiting, reportingDependency: View, transformationTimestamp: Long, successFlagExists: => Boolean, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
   * Transition a view in Transforming state given a succcessful transformation.
   */
  def transformationSucceeded(currentState: Transforming, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
   * Transition a view in Transforming state given a failed transformation.
   */
  def transformationFailed(currentState: Transforming, maxRetries: Int = Schedoscope.settings.retries, currentTime: Long = new Date().getTime): ResultingViewSchedulingState
}

object ViewSchedulingStateMachine {
  
  val noOpLeafViewSchedulingStateMachine = new NoOpLeafViewSchedulingStateMachine

  val noOpIntermediateViewSchedulingStateMachine = new NoOpIntermediateViewSchedulingStateMachine

  val intermediateViewSchedulingStateMachine = new IntermediateViewSchedulingStateMachine

  /**
   * Implicit factory of the view scheduling state machine appropriate for a view's transformation type.
   */
  implicit def schedulingStateMachineForView(view: View) = view.transformation() match {
    case NoOp() => {
      if (view.dependencies.isEmpty)
        noOpLeafViewSchedulingStateMachine
      else
        noOpIntermediateViewSchedulingStateMachine
    }

    case _ => intermediateViewSchedulingStateMachine
  }
}