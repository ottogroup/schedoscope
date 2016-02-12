package org.schedoscope.scheduler.states

import org.schedoscope.dsl.View

sealed abstract class ViewSchedulingStateTransitionAction(view: View)

case class WriteTransformationTimestamp(
  view: View,
  transformationTimestamp: Long) extends ViewSchedulingStateTransitionAction(view)

case class WriteTransformationCheckum(view: View) extends ViewSchedulingStateTransitionAction(view)

case class TouchSuccessFlag(view: View) extends ViewSchedulingStateTransitionAction(view)

case class ReportNoDataAvailable(
  view: View,
  listeners: Set[ViewSchedulingStateListener]) extends ViewSchedulingStateTransitionAction(view)

case class ReportFailed(
  view: View,
  listeners: Set[ViewSchedulingStateListener]) extends ViewSchedulingStateTransitionAction(view)

case class ReportInvalidated(
  view: View,
  listeners: Set[ViewSchedulingStateListener]) extends ViewSchedulingStateTransitionAction(view)

case class ReportMaterialized(
  view: View,
  listeners: Set[ViewSchedulingStateListener],
  transformationTimestamp: Long,
  incomplete: Boolean,
  withErrors: Boolean) extends ViewSchedulingStateTransitionAction(view)

case class DemandMaterialization(
  view: View,
  listeners: Set[ViewSchedulingStateListener]) extends ViewSchedulingStateTransitionAction(view)