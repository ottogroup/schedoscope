package com.ottogroup.bi

import akka.actor.ActorRef
import scala.concurrent.duration.FiniteDuration
import akka.util.Timeout
import akka.pattern.Patterns
import scala.concurrent.Await
import akka.actor.ActorSelection
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

package object soda {
  def queryActor[T](actor: ActorRef, queryMessage: Any, timeoutDuration: FiniteDuration): T = {
    val timeOut = Timeout(timeoutDuration)
    val responseFuture = Patterns.ask(actor, queryMessage, timeOut)
    Await.result(responseFuture, timeOut.duration).asInstanceOf[T]
  }
  
  def queryActorWithMessages[T](actor: ActorRef, queryMessages: List[Any], timeoutDuration: FiniteDuration):  List[T] = {
    val timeOut = Timeout(timeoutDuration)
    
    val responseFutures = queryMessages.map { m => Patterns.ask(actor, m, timeOut) }
    
    implicit val ec = ExecutionContext.global
    val responsesFuture = Future.sequence(responseFutures)
    
    Await.result(responsesFuture, timeOut.duration * queryMessages.size).asInstanceOf[List[T]]
  }
  
  def queryActors[T](actors: List[ActorRef], queryMessage: Any, timeoutDuration: FiniteDuration): List[T] = {
    val timeOut = Timeout(timeoutDuration)
    
    val responseFutures = actors.map {a => Patterns.ask(a, queryMessage, timeOut)}
    
    implicit val ec = ExecutionContext.global
    val responsesFuture = Future.sequence(responseFutures)
    
    Await.result(responsesFuture, timeOut.duration * actors.size).asInstanceOf[List[T]]
  }
}