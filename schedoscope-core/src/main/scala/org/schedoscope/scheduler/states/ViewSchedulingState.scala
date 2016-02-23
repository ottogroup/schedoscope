package org.schedoscope.scheduler.states

import org.schedoscope.dsl.View
import org.schedoscope.scheduler.messages.MaterializeViewMode._

/**
 * Captures the various possible scheduling states views may be in.
 */
sealed abstract class ViewSchedulingState(val view: View)

/**
 * View just has been freshly created by view manager and has never been materialized before.
 */
case class CreatedByViewManager(override val view: View) extends ViewSchedulingState(view)

/**
 * View has been explicitly invalidated and need recomputation
 */
case class Invalidated(override val view: View) extends ViewSchedulingState(view)

/**
 * View has been instantiated by view manager and filled with its prior scheduling state from the schema manager.
 */
case class ReadFromSchemaManager(
  override val view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long) extends ViewSchedulingState(view)

/**
 * View has transformed but has no data.
 */
case class NoData(override val view: View) extends ViewSchedulingState(view)

/**
 * View is waiting for its dependencies to transform.
 */
case class Waiting(
  override val view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long,
  dependenciesMaterializing: Set[View],
  listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange] = Set(),
  materializationMode: MaterializeViewMode = DEFAULT,
  oneDependencyReturnedData: Boolean = false,
  incomplete: Boolean = false,
  withErrors: Boolean = false,
  dependenciesFreshness: Long = 0l) extends ViewSchedulingState(view)

/**
 * View is transforming.
 */
case class Transforming(
  override val view: View,
  lastTransformationChecksum: String,
  listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange] = Set(),
  materializationMode: MaterializeViewMode = DEFAULT,
  withErrors: Boolean,
  incomplete: Boolean,
  retry: Int = 0) extends ViewSchedulingState(view)

/**
 * View has materialized.
 */
case class Materialized(
  override val view: View,
  lastTransformationChecksum: String,
  lastTransformationTimestamp: Long,
  withErrors: Boolean,
  incomplete: Boolean) extends ViewSchedulingState(view)

/**
 * View materialization has failed.
 */
case class Failed(override val view: View) extends ViewSchedulingState(view)

/**
 * View materialization has failed but this is being retried.
 */
case class Retrying(
  override val view: View,
  lastTransformationChecksum: String,
  materializationMode: MaterializeViewMode = DEFAULT,
  listenersWaitingForMaterialize: Set[PartyInterestedInViewSchedulingStateChange] = Set(),
  nextRetry: Int) extends ViewSchedulingState(view)
