package org.schedoscope.scheduler.states

import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.MaterializeViewMode._

/**
 * Captures the various possible scheduling states views may be in.
 */
sealed abstract class ViewSchedulingState(view: View)

/**
 * View just has been freshly created by view manager and has never been materialized before.
 */
case class CreatedByViewManager(view: View) extends ViewSchedulingState(view)

/**
 * View has been explicitly invalidated and need recomputation
 */
case class Invalidated(view: View) extends ViewSchedulingState(view)

/**
 * View has been instantiated by view manager and filled with its prior scheduling state from the schema manager.
 */
case class ReadFromSchemaManager(
  view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long) extends ViewSchedulingState(view)

/**
 * View has transformed but has no data.
 */
case class NoData(
  view: View,
  lastTransformationChecksum: String) extends ViewSchedulingState(view)

/**
 * View is waiting for its dependencies to transform.
 */
case class Waiting(
  view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long,
  dependenciesMaterializing: Set[View],
  listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange] = Set(),
  materializationMode: MaterializeViewMode = DEFAULT,
  oneDependencyReturnedData: Boolean = false,
  incomplete: Boolean = false,
  dependenciesFreshness: Long = 0l) extends ViewSchedulingState(view)

/**
 * View is transforming.
 */
case class Transforming(
  view: View,
  lastTransformationChecksum: String,
  listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange] = Set(),
  materializationMode: MaterializeViewMode = DEFAULT,
  retry: Int = 0) extends ViewSchedulingState(view)

/**
 * View has materialized.
 */
case class Materialized(
  view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long) extends ViewSchedulingState(view)

/**
 * View materialization has failed.
 */
case class Failed(view: View) extends ViewSchedulingState(view)

/**
 * View materialization has failed but this is being retried.
 */
case class Retrying(
  view: View,
  lastTransformationChecksum: String,
  materializationMode: MaterializeViewMode = DEFAULT,
  listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange] = Set(),
  nextRetry: Int) extends ViewSchedulingState(view)
