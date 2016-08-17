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

import java.util.Date

import org.schedoscope.Schedoscope
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.MaterializeViewMode._

import scala.language.implicitConversions

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
    */
  def materialize(currentState: ViewSchedulingState, issuer: PartyInterestedInViewSchedulingStateChange, materializationMode: MaterializeViewMode = DEFAULT, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
    * Given the view's current view scheduling state apply an invalidate command. This issuer of the command is passed along as an interested listener in the outcome.
    */
  def invalidate(currentState: ViewSchedulingState, issuer: PartyInterestedInViewSchedulingStateChange): ResultingViewSchedulingState

  /**
    * Restart a retrying view's materialization
    */
  def retry(currentState: Retrying): ResultingViewSchedulingState

  /**
    * Apply a NoData report of a dependency to the current scheduling state of a waiting view.
    *
    */
  def noDataAvailable(currentState: Waiting, reportingDependency: View, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
    * Apply a Failed report of a dependency to the current scheduling state of a waiting view.
    *
    */
  def failed(currentState: Waiting, reportingDependency: View, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
    * Apply a Materialized report of a dependency to the current scheduling state of a waiting view.
    *
    */
  def materialized(currentState: Waiting, reportingDependency: View, transformationTimestamp: Long, withErrors: Boolean, incomplete: Boolean, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
    * Transition a view in Transforming state given a successful transformation.
    */
  def transformationSucceeded(currentState: Transforming, folderEmpty: => Boolean, currentTime: Long = new Date().getTime): ResultingViewSchedulingState

  /**
    * Transition a view in Transforming state given a failed transformation.
    */
  def transformationFailed(currentState: Transforming, maxRetries: Int = Schedoscope.settings.retries, currentTime: Long = new Date().getTime): ResultingViewSchedulingState
}