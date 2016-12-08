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
package org.schedoscope.scheduler.listeners

import org.schedoscope.scheduler.messages.MaterializeViewMode.MaterializeViewMode
import org.schedoscope.scheduler.states._
import org.schedoscope.dsl.View


/**
  * Trait for user defined code to be executed before and after a given View state.
  * e.g. for gathering statistics and logging information about View status
  *
  */
trait ViewSchedulingListener {

  val INIT = "MONITORING:"
  var latestViewEvent = Map[View, ViewSchedulingEvent]()

  /**
    * Called on every incoming View scheduling event
    */
  def viewSchedulingEvent(event: ViewSchedulingEvent): Unit

  def getMonitInit(view: View) = s"${INIT} VIEW [${view.n}] "

  def getViewStateChangeInfo(event: ViewSchedulingEvent) =
    s"STATE CHANGE ===> ${event.newState.label.toUpperCase()}: " +
      s"newState=${event.newState} previousState=${event.prevState}"

  def getSetOfActions(event: ViewSchedulingEvent) =
    s"scheduled actions to perform: [${event.actions.toList.mkString(", ")}]"

  def parseAnyState(state: ViewSchedulingState) =
    state match {
      case CreatedByViewManager(view) =>
        "initialised"
      case Invalidated(view) =>
        "invalidated"
      case NoData(view) =>
        "has no data available for materialization"
      case _ => getStateMetrics(state)
    }

  def getTransformationDetails(transfChecksum: String, transfTimestamp: Option[Long]) = {
    val ts = transfTimestamp match {
      case Some(t:Long) => s"\ttransformation-timestamp: ${t}"
      case _ => ""
    }
    s"\ttransformation-checksum: ${transfChecksum}" + ts
  }

  def getWithErrorsOrIncomplete(errors: Boolean, incomplete: Boolean) =
      s"\terrors: ${errors}" +
      s"\tincomplete: ${incomplete}"

  def getMaterializationDetails(listenersWaitingForMaterialize: scala.collection.Set[PartyInterestedInViewSchedulingStateChange],
                   materializationMode: MaterializeViewMode) =
    s"\tlisteners-waiting-for-this-materialization: [${listenersWaitingForMaterialize.mkString(", ")}]" +
    s"\tmaterialization-mode: ${materializationMode}"

  def getStateMetrics(state: ViewSchedulingState) = {
    val msg = state match {
      case Materialized(view, transfChecksum, transfTimestamp, errors, incomplete) =>
        getTransformationDetails(transfChecksum, Some(transfTimestamp)) +
          getWithErrorsOrIncomplete(errors, incomplete)

      case ReadFromSchemaManager(view, transfChecksum, transfTimestamp) =>
        getTransformationDetails(transfChecksum, Some(transfTimestamp))

      case Waiting(view, transfChecksum, transfTimestamp, dependenciesMaterializing,
        listenersWaitingForMaterialize, materializationMode, oneDependencyReturnedData,
        errors, incomplete, dependenciesFreshness) =>
        getTransformationDetails(transfChecksum, Some(transfTimestamp)) +
          s"\tdependencies-materializing: ${dependenciesMaterializing.mkString(", ")}" +
          getMaterializationDetails(listenersWaitingForMaterialize, materializationMode) +
          s"\tone-dependency-returned-data: ${oneDependencyReturnedData}" +
          getWithErrorsOrIncomplete(errors, incomplete) +
          s"\tdependencies-freshness: ${dependenciesFreshness}"

      case Transforming(view, transfChecksum, listenersWaitingForMaterialize,
        materializationMode, errors, incomplete, retries) =>
        getTransformationDetails(transfChecksum, None) +
          getMaterializationDetails(listenersWaitingForMaterialize, materializationMode) +
          getWithErrorsOrIncomplete(errors, incomplete) +
          s"\tretries: ${retries}"

      case Retrying(view, transfChecksum, listenersWaitingForMaterialize,
        materializationMode, errors, incomplete, nextRetry) =>
          getTransformationDetails(transfChecksum, None) +
          getMaterializationDetails(listenersWaitingForMaterialize, materializationMode) +
          getWithErrorsOrIncomplete(errors, incomplete) +
          s"\tnext-retry: ${nextRetry}"
      case _ => "no details available for this state"
    }
    s"STATE [${state.label.toUpperCase()}] DETAILS: " + msg
  }

  def storeNewEvent(event: ViewSchedulingEvent) =
    latestViewEvent += (event.prevState.view -> event)

  def getViewSchedulingTimeDelta(event: ViewSchedulingEvent):Long = {
    if(latestViewEvent contains(event.prevState.view)) {
      val prevT = latestViewEvent.get(event.prevState.view).get.eventTime.toDateTime.getMillis / 1000
      val t = event.eventTime.toDateTime().getMillis / 1000
      t - prevT
    } else 0
  }

  def getViewSchedulingTimeDeltaOutput(event:ViewSchedulingEvent) =
    if(latestViewEvent contains(event.prevState.view)) {
      val prevEvent = latestViewEvent.get(event.prevState.view).get
      val tStamp = if(getViewSchedulingTimeDelta(event) > 0)
        s"\tevents timestampt difference: ${getViewSchedulingTimeDelta(event)} (s)"
      else
        ""
      s"previous event state change: ['${prevEvent.prevState.label}' " +
        s"===> '${prevEvent.newState.label}'] \tcurrent event state change: " +
        s"['${prevEvent.prevState.label}' ===> '${prevEvent.newState.label}'] " +
        tStamp
    } else ""

}
