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
package org.schedoscope

import akka.actor.ActorSystem
import org.schedoscope.scheduler.actors.{SchemaManagerRouter, TransformationManagerActor, ViewManagerActor, ViewSchedulingListenerManagerActor}

/**
  * The Schedoscope object provides accessors for the various components of the schedoscope system.
  * It thus serves as an entry point to my system.
  */
object Schedoscope {

  /**
    * Pluggable builder function that returns the actor system for schedoscope.
    * The default implementation creates a new actor system.
    */
  var actorSystemBuilder = () => ActorSystem("schedoscope")

  /**
    * Pluggable builder function that returns the view manager actor for schedoscope.
    * The default implementation creates a new view manager actor based on the actor system.
    */
  var viewManagerActorBuilder = () => actorSystem.actorOf(
    ViewManagerActor.props(settings,
      transformationManagerActor,
      schemaManagerRouter), "views")

  /**
    * Pluggable builder function that returns the settings for schedoscope.
    * The default implementation creates settings using the settings of the actor system.
    */
  var settingsBuilder = () => Settings()

  /**
    * Pluggable builder function that returns the transformation manager actor for schedoscope.
    * The default implementation creates a new transformation manager actor based on the actor system.
    */
  var transformationManagerActorBuilder = () => actorSystem.actorOf(TransformationManagerActor.props(settings), "transformations")

  /**
    * Pluggable builder function that returns the schema manager actor for schedoscope.
    * The default implementation creates a new schema manager router based on the actor system.
    */
  var schemaManagerRouterBuilder = () => actorSystem.actorOf(SchemaManagerRouter.props(settings), "schema")

  /**
    * Pluggable builder function that returns the view scheduling listener manager actor for schedoscope.
    * The default implementation creates a new view scheduling listener manager based on the actor system.
    */
  var viewSchedulingListenerManagerActorBuilder = () => actorSystem.actorOf(
    ViewSchedulingListenerManagerActor.props(settings),
    "ViewSchedulingListenerManagerActor")

  /**
    * The Schedoscope actor system
    */
  lazy val actorSystem = actorSystemBuilder()

  /**
    * The Schedoscope settings.
    */
  lazy val settings = settingsBuilder()

  /**
    * A reference to the Schedoscope transformation manager logger actor
    */
  lazy val transformationManagerActor = transformationManagerActorBuilder()

  /**
    * A reference to the Schedoscope schema manager actor
    */
  lazy val schemaManagerRouter = schemaManagerRouterBuilder()

  /**
    * A reference to the Schedoscope view manager actor
    */
  lazy val viewManagerActor = viewManagerActorBuilder()

  /**
    * A reference to the Schedoscope view scheduling listener manager actor
    */
  lazy val viewSchedulingListenerManagerActor = viewSchedulingListenerManagerActorBuilder()
}