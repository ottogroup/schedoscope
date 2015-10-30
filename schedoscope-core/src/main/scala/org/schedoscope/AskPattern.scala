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

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.pattern.Patterns
import akka.util.Timeout

/**
 * Contains commonly codified ask patterns for actors.
 */
object AskPattern {
  implicit val executionContext: ExecutionContext = Schedoscope.actorSystem.dispatchers.lookup("akka.actor.future-call-dispatcher")

  /**
   * Query an actor for a response by sending a message and observing a timeout.
   */
  def queryActor[T](actor: ActorRef, queryMessage: Any, timeoutDuration: FiniteDuration): T = {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))
    val responseFuture = Patterns.ask(actor, queryMessage, askTimeOut)
    Await.result(responseFuture, waitTimeOut.duration).asInstanceOf[T]
  }

  /**
   * Query an actor for responses by sending it multiple messages and observing a timeout.
   */
  def queryActors[T](actor: ActorRef, queryMessages: List[Any], timeoutDuration: FiniteDuration): List[T] = {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))

    val responseFutures = queryMessages.map { m => Patterns.ask(actor, m, askTimeOut) }

    val responsesFuture = Future.sequence(responseFutures)

    Await.result(responsesFuture, waitTimeOut.duration * queryMessages.size).asInstanceOf[List[T]]
  }

  /**
   * Query multiple actors for responses by sending them a common message observing a timeout.
   */
  def queryActors[T](actors: List[ActorRef], queryMessage: Any, timeoutDuration: FiniteDuration): List[T] = {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))
    val responseFutures = actors.map { a => Patterns.ask(a, queryMessage, askTimeOut) }

    val responsesFuture = Future.sequence(responseFutures)

    Await.result(responsesFuture, waitTimeOut.duration * actors.size).asInstanceOf[List[T]]
  }

  /**
   * Transform an actor selection to an actor ref via an ask pattern.
   */
  def actorSelectionToRef(actorSelection: ActorSelection, timeoutDuration: FiniteDuration = Schedoscope.settings.viewManagerResponseTimeout): Option[ActorRef] = try {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))
    Some(Await.result(actorSelection.resolveOne(askTimeOut.duration), waitTimeOut.duration))
  } catch {
    case _: Throwable => None
  }
}