package org.schedoscope

import java.util.concurrent.TimeUnit

import scala.annotation.implicitNotFound
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import org.schedoscope.scheduler.RootActor

import akka.actor.ActorRef
import akka.pattern.Patterns
import akka.util.Timeout

package object scheduler {
  implicit val executionContext: ExecutionContext = RootActor.settings.system.dispatchers.lookup("akka.actor.future-call-dispatcher")

  def queryActor[T](actor: ActorRef, queryMessage: Any, timeoutDuration: FiniteDuration): T = {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))
    val responseFuture = Patterns.ask(actor, queryMessage, askTimeOut)
    Await.result(responseFuture, waitTimeOut.duration).asInstanceOf[T]
  }

  def queryActors[T](actor: ActorRef, queryMessages: List[Any], timeoutDuration: FiniteDuration): List[T] = {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))

    val responseFutures = queryMessages.map { m => Patterns.ask(actor, m, askTimeOut) }

    val responsesFuture = Future.sequence(responseFutures)

    Await.result(responsesFuture, waitTimeOut.duration * queryMessages.size).asInstanceOf[List[T]]
  }

  def queryActors[T](actors: List[ActorRef], queryMessage: Any, timeoutDuration: FiniteDuration): List[T] = {
    val askTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.1).toLong, TimeUnit.MILLISECONDS))
    val waitTimeOut = Timeout(FiniteDuration((timeoutDuration.toMillis * 1.2).toLong, TimeUnit.MILLISECONDS))
    val responseFutures = actors.map { a => Patterns.ask(a, queryMessage, askTimeOut) }

    val responsesFuture = Future.sequence(responseFutures)

    Await.result(responsesFuture, waitTimeOut.duration * actors.size).asInstanceOf[List[T]]
  }
}