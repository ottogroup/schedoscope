package org.schedoscope.scheduler.states

import org.schedoscope.dsl.View

/**
 * Abstract class to capture the various actions to be performed upon a reaching a view schedulings state result.
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
case class Materialize(view: View, requester: PartyInterestedInViewSchedulingStateChange) extends ViewSchedulingAction(view)

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
 * All parties interested in the view scheduling state should be notified that the view has materialized.
 */
case class ReportMaterialized(
  view: View,
  listeners: Set[PartyInterestedInViewSchedulingStateChange],
  transformationTimestamp: Long,
  incomplete: Boolean,
  withErrors: Boolean) extends ViewSchedulingAction(view)