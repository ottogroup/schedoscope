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

/**
 * Abstract class to capture the various actions to be performed upon a reaching a view scheduling state result.
 */
sealed abstract class ViewSchedulingAction(view: View)

/**
 * A new view transformation timestamp has to be written to the metastore
 */
case class WriteTransformationTimestamp(
  view: View,
  transformationTimestamp: Long) extends ViewSchedulingAction(view)

/**
 * The new view transformation checksum has to be written to the metastore
 */
case class WriteTransformationCheckum(view: View) extends ViewSchedulingAction(view)

/**
 * A success flag needs to be written to HDFS into a view's partition folder
 */
case class TouchSuccessFlag(view: View) extends ViewSchedulingAction(view)

/**
 * A view should materialize.
 */
case class Materialize(view: View, requester: PartyInterestedInViewSchedulingStateChange, materializationMode: MaterializeViewMode) extends ViewSchedulingAction(view)

/**
 * A view should transform.
 */
case class Transform(view: View) extends ViewSchedulingAction(view)

/**
 * All parties interested in the view scheduling state should be notified that no data is available for the view.
 */
case class ReportNoDataAvailable(
  view: View,
  listeners: Set[PartyInterestedInViewSchedulingStateChange]) extends ViewSchedulingAction(view)

/**
 * All parties interested in the view scheduling state should be notified that the view transformation has failed.
 */
case class ReportFailed(
  view: View,
  listeners: Set[PartyInterestedInViewSchedulingStateChange]) extends ViewSchedulingAction(view)

/**
 * All parties interested in the view scheduling state should be notified that the view has been invalidated.
 */
case class ReportInvalidated(
  view: View,
  listeners: Set[PartyInterestedInViewSchedulingStateChange]) extends ViewSchedulingAction(view)

/**
 * All parties interested in the view scheduling state should be notified that the view has not been invalidated.
 */
case class ReportNotInvalidated(
  view: View,
  listeners: Set[PartyInterestedInViewSchedulingStateChange]) extends ViewSchedulingAction(view)

/**
 * All parties interested in the view scheduling state should be notified that the view has materialized.
 */
case class ReportMaterialized(
  view: View,
  listeners: Set[PartyInterestedInViewSchedulingStateChange],
  transformationTimestamp: Long,
  withErrors: Boolean,
  incomplete: Boolean) extends ViewSchedulingAction(view)