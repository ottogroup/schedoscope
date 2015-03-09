package com.ottogroup.bi.soda.bottler.api


import scala.util.{Success, Failure}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.event.Logging
import akka.io.IO
import spray.can.Http
import spray.httpx.SprayJsonSupport
import spray.client.pipelining._
import spray.util._
import spray.http.HttpResponse
import scala.concurrent.impl.Future
import scala.concurrent.Future
import spray.http.HttpResponse
import scala.util.Try
import akka.util.Timeout
import scala.concurrent.Await
import spray.http.HttpRequest
import spray.json.DefaultJsonProtocol
import spray.httpx.SprayJsonSupport._
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser

object SodaRestClient {
  
  var host = "localhost"
  var port = 20699
  
  import SodaJsonProtocol._
  
  implicit val system = ActorSystem("soda-spray-client")  
  import system.dispatcher // execution context for futures below
  implicit val timeout = Timeout(120.seconds)
  val log = Logging(system, getClass)
    
  def get[T] (q: String) : Future[T] = {
    val pipeline = q match {
      case u : String if u.startsWith("/listactions/") => sendReceive ~> unmarshal[ProcList]
      case u : String if u.startsWith("/listviews/") => sendReceive ~> unmarshal[ViewList]
      case u : String if u.startsWith("/materialize/") => sendReceive ~> unmarshal[ViewStat]
      case _ => throw new RuntimeException("Unsupported query: " + q)
    }
    log.info("Querying: " + url(q))
    pipeline(Get(url(q))).asInstanceOf[Future[T]]
  }
  
  private def url(u: String) = {
    s"http://${host}:${port}${u}"
  }
  
  def close() {
    system.shutdown()
  }
}


/**
 * @author dbenz
 */
object SodaClient {
           
  def listActions = Await.result(SodaRestClient.get[ProcList]("/listactions/any"), 20.seconds)
  
  def listViews = Await.result(SodaRestClient.get[ViewList]("/listviews/any"), 20.seconds)
  
  def materialize(env: String, db: String, view: String, params: String) = {
   val viewUrlPath = s"/materialize/${env}/${db}/${view}/${params}"
   ViewUrlParser.parse(viewUrlPath) // throws exception on failure
   Await.result(SodaRestClient.get[ViewStat](s"/materialize/${viewUrlPath}"), 10.days) 
  }
  
  def close() {
    SodaRestClient.close()
  }
  
  show processlist
}


object SodaControl extends App {
  // TODO implement CLI arg browsing, call respective functions from SodaClient
  
  
}
