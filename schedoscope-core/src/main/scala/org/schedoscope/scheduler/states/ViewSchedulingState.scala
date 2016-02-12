package org.schedoscope.scheduler.states

import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.MaterializeViewMode._

sealed abstract class ViewSchedulingState(view: View)

case class ReadFromSchemaManager(
  view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long) extends ViewSchedulingState(view)

case class NoData(
  view: View,
  lastTransformationChecksum: String) extends ViewSchedulingState(view)

case class CreatedByViewManager(view: View) extends ViewSchedulingState(view)

case class Invalidated(view: View) extends ViewSchedulingState(view)

case class Waiting(
  view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long,
  dependenciesMaterializing: Set[View],
  dependendentsWaitingForMaterialize: Set[View] = Set(),
  materializationMode: MaterializeViewMode = DEFAULT,
  oneDependencyReturnedData: Boolean = false,
  incomplete: Boolean = false,
  dependenciesFreshness: Long = 0l) extends ViewSchedulingState(view)

case class Transforming(
  view: View,
  lastTransformationChecksum: String,
  dependendentsWaitingForMaterialize: Set[View] = Set(),
  materializationMode: MaterializeViewMode = DEFAULT,
  retry: Int = 0) extends ViewSchedulingState(view)

case class Materialized(
  view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long) extends ViewSchedulingState(view)

case class Failed(view: View) extends ViewSchedulingState(view)

case class Retrying(
  view: View,
  lastTransformationChecksum: String,
  materializationMode: MaterializeViewMode = DEFAULT,
  dependendentsWaitingForMaterialize: Set[View] = Set(),
  nextRetry: Int) extends ViewSchedulingState(view)