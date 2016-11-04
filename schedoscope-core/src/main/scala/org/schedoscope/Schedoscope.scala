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
import org.schedoscope.scheduler.actors.{SchemaManagerRouter, TransformationManagerActor, ViewManagerActor}

/**
  * The Schedoscope object provides accessors for the various components of the schedoscope system.
  * It thus serves as an entry point to my system.
  */
object Schedoscope {

  var actorSystemBuilder = () => ActorSystem("schedoscope")

  var viewManagerActorBuilder = () => actorSystem.actorOf(
    ViewManagerActor.props(settings,
      transformationManagerActor,
      schemaManagerRouter), "views")

  /**
    * The Schedoscope actor system
    */
  lazy val actorSystem = actorSystemBuilder()

  /**
    * The Schedoscope settings.
    */
  lazy val settings = Settings()

  /**
    * A reference to the Schedoscope transformation manager logger actor
    */
  lazy val transformationManagerActor = actorSystem.actorOf(TransformationManagerActor.props(settings), "transformations")

  /**
    * A reference to the Schedoscope schema manager actor
    */
  lazy val schemaManagerRouter = actorSystem.actorOf(SchemaManagerRouter.props(settings), "schema")



  /**
    * A reference to the Schedoscope view manager actor
    */
  lazy val viewManagerActor = viewManagerActorBuilder()
}