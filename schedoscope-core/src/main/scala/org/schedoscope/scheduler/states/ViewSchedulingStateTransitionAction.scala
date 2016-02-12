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
  dependendents: Set[View]) extends ViewSchedulingStateTransitionAction(view)

case class ReportFailed(
  view: View,
  dependendents: Set[View]) extends ViewSchedulingStateTransitionAction(view)

case class ReportMaterialized(
  view: View,
  dependendents: Set[View],
  transformationTimestamp: Long,
  incomplete: Boolean,
  withErrors: Boolean) extends ViewSchedulingStateTransitionAction(view)

case class DemandMaterialization(
  view: View,
  dependencies: Set[View]) extends ViewSchedulingStateTransitionAction(view)