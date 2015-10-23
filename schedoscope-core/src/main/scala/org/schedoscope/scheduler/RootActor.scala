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
package org.schedoscope.scheduler

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.schedoscope.Settings
import org.schedoscope.SettingsImpl
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy._
import akka.event.Logging

/**
 * Root actor of the schedoscope scheduler actor system.
 * Merely a supervisor that shuts down schedoscope in case anything gets escalated.
 */
class RootActor(settings: SettingsImpl) extends Actor {
  import context._

  val log = Logging(system, this)

  var transformationManagerActor: ActorRef = null
  var schemaRootActor: ActorRef = null
  var viewManagerActor: ActorRef = null

  override val supervisorStrategy =
    AllForOneStrategy() {
      case t: Throwable => {
        t.printStackTrace()
        this.context.system.shutdown()
        Escalate
      }
    }

  override def preStart {
    transformationManagerActor = actorOf(TransformationManagerActor.props(settings.hadoopConf), "transformations")
    schemaRootActor = actorOf(SchemaRootActor.props(settings), "schema-root")
    viewManagerActor = actorOf(
      ViewManagerActor.props(settings,
        transformationManagerActor,
        schemaRootActor,
        schemaRootActor), "views")
  }

  def receive = {
    // we do not process any messages as we are merely a supervisor
    case _ => {}
  }
}

/**
 * Helpful constants to access the various actors in the schedoscope actor systems. These implicitly create
 * the actors upon first request.
 */
object RootActor {
  def props(settings: SettingsImpl) = Props(classOf[RootActor], settings).withDispatcher("akka.actor.root-actor-dispatcher")

  lazy val settings = Settings()

  def actorSelectionToRef(actorSelection: ActorSelection): Option[ActorRef] = try {
    Some(Await.result(actorSelection.resolveOne(settings.viewManagerResponseTimeout), settings.viewManagerResponseTimeout))
  } catch {
    case _: Throwable => None
  }

  lazy val rootActor = actorSelectionToRef(settings.system.actorSelection(settings.system.actorOf(props(settings), "root").path)).get

  lazy val viewManagerActor = actorSelectionToRef(settings.system.actorSelection(rootActor.path.child("views"))).get

  lazy val schemaRootActor = actorSelectionToRef(settings.system.actorSelection(rootActor.path.child("schema-root"))).get

  lazy val schemaActor = actorSelectionToRef(settings.system.actorSelection(schemaRootActor.path.child("schema"))).get

  lazy val metadataLoggerActor = actorSelectionToRef(settings.system.actorSelection(schemaRootActor.path.child("metadata-logger"))).get

  lazy val transformationManagerActor = actorSelectionToRef(settings.system.actorSelection(rootActor.path.child("transformations"))).get
}