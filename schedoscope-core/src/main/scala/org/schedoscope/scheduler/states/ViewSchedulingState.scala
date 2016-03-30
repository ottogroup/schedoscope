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

import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.MaterializeViewMode._
import scala.collection.Set

/**
 * Captures the various possible scheduling states views may be in.
 */
sealed abstract class ViewSchedulingState(val view: View) {
  /**
   * A string label for the type of state.
   */
  def label: String
}

/**
 * View just has been freshly created by view manager and has never been materialized before.
 */
case class CreatedByViewManager(override val view: View) extends ViewSchedulingState(view) {

  override def label = "receive"

}

/**
 * View has been explicitly invalidated and need recomputation
 */
case class Invalidated(override val view: View) extends ViewSchedulingState(view) {

  override def label = "invalidated"

}

/**
 * View has been instantiated by view manager and filled with its prior scheduling state from the schema manager.
 */
case class ReadFromSchemaManager(
    override val view: View,
    lastTransformationChecksum: String,
    lastTransformationTimestamp: Long) extends ViewSchedulingState(view) {

  override def label = "receive"

}

/**
 * View has transformed but has no data.
 */
case class NoData(override val view: View) extends ViewSchedulingState(view) {

  override def label = "nodata"

}

/**
 * View is waiting for its dependencies to transform.
 */
case class Waiting(
    override val view: View,
    lastTransformationChecksum: String,
    lastTransformationTimestamp: Long,
    dependenciesMaterializing: Set[View],
    listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange],
    materializationMode: MaterializeViewMode,
    oneDependencyReturnedData: Boolean = false,
    withErrors: Boolean = false,
    incomplete: Boolean = false,
    dependenciesFreshness: Long = 0l) extends ViewSchedulingState(view) {

  override def label = "waiting"

}

/**
 * View is transforming.
 */
case class Transforming(
    override val view: View,
    lastTransformationChecksum: String,
    listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange],
    materializationMode: MaterializeViewMode,
    withErrors: Boolean,
    incomplete: Boolean,
    retry: Int = 0) extends ViewSchedulingState(view) {

  override def label = "transforming"

}

/**
 * View has materialized.
 */
case class Materialized(
    override val view: View,
    lastTransformationChecksum: String,
    lastTransformationTimestamp: Long,
    withErrors: Boolean,
    incomplete: Boolean) extends ViewSchedulingState(view) {

  override def label = "materialized"

}

/**
 * View materialization has failed.
 */
case class Failed(override val view: View) extends ViewSchedulingState(view) {

  override def label = "failed"

}

/**
 * View materialization has failed but this is being retried.
 */
case class Retrying(
    override val view: View,
    lastTransformationChecksum: String,
    listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange],
    materializationMode: MaterializeViewMode,
    withErrors: Boolean,
    incomplete: Boolean,
    nextRetry: Int) extends ViewSchedulingState(view) {

  override def label = "retrying"

}
