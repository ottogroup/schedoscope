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

import akka.actor.{ActorRef, ActorSelection}
import akka.pattern.{Patterns, AskTimeoutException}
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}

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
    * Executes an ask pattern with retries in case it yield Timeout or AskTimeout exceptions. These exceptions will only
    * be thrown after the last retry. All other exceptions will be thrown immediately.
    *
    * @param pattern a function with the ask pattern to potentially retry
    * @param retries the number of retries, defaulting to max int for infinite retries
    * @return the result of the ask operation after the given number of retries.
    */
  def retryOnTimeout[R](pattern: () => R, retries: Int = Int.MaxValue): R = try {
    pattern()
  } catch {
    case t: TimeoutException => if (retries > 0) retryOnTimeout(pattern, retries - 1) else throw t
    case at: AskTimeoutException => if (retries > 0) retryOnTimeout(pattern, retries - 1) else throw at
    case o: Throwable => throw o
  }
}