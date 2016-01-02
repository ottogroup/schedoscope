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
package org.schedoscope.scheduler.rest.client

import akka.actor.ActorSystem
import akka.event.Logging
import akka.util.Timeout
import org.schedoscope.scheduler.rest.SchedoscopeJsonDataFormat
import org.schedoscope.scheduler.service.{ QueueStatusList, SchedoscopeCommandStatus, SchedoscopeService, TransformationStatusList, ViewStatusList }
import spray.client.pipelining.{ Get, WithTransformerConcatenation, sendReceive, unmarshal }
import spray.http.Uri
import spray.httpx.SprayJsonSupport.sprayJsonUnmarshaller

import scala.collection.immutable.Map
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Implementation of the schedoscope service that maps the given scheduling commands
 * to REST web service calls and parse the returned results.
 */
class SchedoscopeServiceRestClientImpl(val host: String, val port: Int) extends SchedoscopeService {

  import SchedoscopeJsonDataFormat._

  implicit val system = ActorSystem("schedoscope-rest-client")

  import system.dispatcher

  // execution context for futures below
  implicit val timeout = Timeout(10.days)
  val log = Logging(system, getClass)

  def get[T](path: String, query: Map[String, String]): Future[T] = {
    val pipeline = path match {
      case u: String if u.startsWith("/views") => sendReceive ~> unmarshal[ViewStatusList]
      case u: String if u.startsWith("/transformations") => sendReceive ~> unmarshal[TransformationStatusList]
      case u: String if u.startsWith("/queues") => sendReceive ~> unmarshal[QueueStatusList]
      case u: String if u.startsWith("/materialize") => sendReceive ~> unmarshal[SchedoscopeCommandStatus]
      case u: String if u.startsWith("/invalidate") => sendReceive ~> unmarshal[SchedoscopeCommandStatus]
      case u: String if u.startsWith("/newdata") => sendReceive ~> unmarshal[SchedoscopeCommandStatus]
      case u: String if u.startsWith("/commands") => sendReceive ~> unmarshal[List[SchedoscopeCommandStatus]]
      case _ => throw new RuntimeException("Unsupported query path: " + path)
    }
    val uri = Uri.from("http", "", host, port, path) withQuery (query)
    println("Calling Schedoscope API URL: " + uri)
    pipeline(Get(uri)).asInstanceOf[Future[T]]
  }

  private def paramsFrom(params: (String, Option[Any])*): Map[String, String] = {
    params.filter(_._2.isDefined)
      .map(p => (p._1 -> p._2.get.toString))
      .toMap
  }

  def shutdown(): Boolean = {
    system.shutdown()
    system.isTerminated
  }

  def materialize(viewUrlPath: Option[String], status: Option[String], filter: Option[String], mode: Option[String]): SchedoscopeCommandStatus = {
    Await.result(get[SchedoscopeCommandStatus](s"/materialize/${viewUrlPath.getOrElse("")}", paramsFrom(("status", status), ("filter", filter), ("mode", mode))), 10.days)
  }

  def invalidate(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean]): SchedoscopeCommandStatus = {
    Await.result(get[SchedoscopeCommandStatus](s"/invalidate/${viewUrlPath.getOrElse("")}", paramsFrom(("status", status), ("filter", filter), ("dependencies", dependencies))), 3600 seconds)
  }

  def newdata(viewUrlPath: Option[String], status: Option[String], filter: Option[String]): SchedoscopeCommandStatus = {
    Await.result(get[SchedoscopeCommandStatus](s"/newdata/${viewUrlPath.getOrElse("")}", paramsFrom(("status", status), ("filter", filter))), 3600 seconds)
  }

  def commandStatus(commandId: String): SchedoscopeCommandStatus = {
    null
  } // TODO

  def commands(status: Option[String], filter: Option[String]): List[SchedoscopeCommandStatus] = {
    Await.result(get[List[SchedoscopeCommandStatus]](s"/commands", paramsFrom(("status", status), ("filter", filter))), 3600 seconds)
  }

  def views(viewUrlPath: Option[String], status: Option[String], filter: Option[String], dependencies: Option[Boolean], overview: Option[Boolean]): ViewStatusList = {
    Await.result(get[ViewStatusList](s"/views/${viewUrlPath.getOrElse("")}", paramsFrom(("status", status), ("filter", filter), ("dependencies", dependencies), ("overview", overview))), 3600 seconds)
  }

  def transformations(status: Option[String], filter: Option[String]): TransformationStatusList = {
    Await.result(get[TransformationStatusList](s"/transformations", paramsFrom(("status", status), ("filter", filter))), 3600 seconds)
  }

  def queues(typ: Option[String], filter: Option[String]): QueueStatusList = {
    Await.result(get[QueueStatusList](s"/queues", paramsFrom(("typ", typ), ("filter", filter))), 3600 seconds)
  }
}