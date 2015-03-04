package com.ottogroup.bi.soda.bottler.api

import colossus._
import service._
import protocols.http._
import HttpMethod._
import UrlParsing.Strings._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Promise, Future }
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import Predef.{ any2stringadd => _, _ }
import colossus.core.ServerSettings
import com.ottogroup.bi.soda.bottler.NewDataAvailable
import com.ottogroup.bi.soda.bottler.ViewStatus
import com.ottogroup.bi.soda.bottler.KillAction
import com.ottogroup.bi.soda.bottler.InternalError
import com.ottogroup.bi.soda.bottler.ViewMaterialized
import com.ottogroup.bi.soda.bottler.SchemaActor
import com.ottogroup.bi.soda.bottler.NoDataAvailable
import com.ottogroup.bi.soda.bottler.Deploy
import com.ottogroup.bi.soda.bottler.ViewSuperVisor
import com.ottogroup.bi.soda.bottler.ActionsRouterActor
import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser._
import com.ottogroup.bi.soda.bottler.GetStatus
import com.ottogroup.bi.soda.bottler.ProcessList
import com.ottogroup.bi.soda.bottler.ProcessStatus._
import com.ottogroup.bi.soda.bottler.HiveStatusResponse
import com.ottogroup.bi.soda.bottler.OozieStatusResponse
import com.ottogroup.bi.soda.bottler.ViewStatusResponse
import com.ottogroup.bi.soda.bottler.Failed
import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import akka.actor.ActorRef
import com.ottogroup.bi.soda.bottler.ViewStatusRetriever
import com.ottogroup.bi.soda.bottler.ViewStatusRetriever
import com.ottogroup.bi.soda.bottler.ViewStatusResponse
import org.codehaus.jackson.map.ObjectMapper
import com.cloudera.com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.io.JsonStringEncoder

object SodaService {
  val settings = Settings()
  
  val om = new ObjectMapper()
  val enc = JsonStringEncoder.getInstance
  
  val headers = List(("Content-Type", "application/json"))

  implicit val io = IOSystem()
  implicit val ec = ExecutionContext.global
  implicit val timeout = Timeout(3 days) // needed for `?` below

  val supervisor = settings.system.actorOf(ViewSuperVisor.props(settings), "supervisor")
  val scheduleActor = settings.system.actorOf(ActionsRouterActor.props(settings.hadoopConf), "actions")
  val schemaActor = settings.system.actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "schemaActor")

  val viewAugmentor = if (settings.parsedViewAugmentorClass != "")
    Class.forName(settings.parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]
  else
    null
  val formatter = DateTimeFormat.fullDateTime()

  def start() {    
    Service.serve[Http]("http-service", settings.port, settings.webserviceTimeOut) {
      (context =>
        context.handle { connection =>
          connection.become {
            case request @ Get on Root => sendOk(request, """{"status" : "ok"}""")

            case request @ Get on Root /: "materialize" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => viewActor ? "materialize")
                val res = Await.result(Future sequence fut,  10 days)
                val result = res.foldLeft(0) { (count, r) =>
                  r match {
                    case ViewMaterialized(v, incomplete, changed,errors) => count + 1
                    case _: NoDataAvailable => count
                    case Failed(view) => count
                  }
                }
                if (result == res.size)
                  sendOk(request, s"""{ "status":"success", "view":"${viewName(viewUrlPath)}}"""")
                else if (result == 0)
                  sendOk(request, s"""{ "status":"nodata", "view":"${viewName(viewUrlPath)}}"""")
                else
                  sendOk(request, s"""{ "status":"incomplete", "view":"${viewName(viewUrlPath)}}"""")
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "status" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? "status").mapTo[ViewStatus])
                val res = Await.result(Future sequence fut, 1 hour)
                val resultMessage = res.foldLeft("") { (message, current) => message + current.view.toString() + "\n" }
                request.ok(resultMessage, headers)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "invalidate" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? "invalidate").mapTo[View])
                val res = Await.result(Future sequence fut, 1 hour)
                val resultMessage = res.foldLeft("") { (message, current) => message + current.toString() + "\n" }
                request.ok(resultMessage, headers)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "newdata" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? "status").mapTo[ViewStatus])
                val viewActorsStatus = Await.result(Future sequence fut, 1 hour)
                viewActors.zip(viewActorsStatus).foreach { case (a, s) => a ! NewDataAvailable(s.view) }
                request.ok("ok", headers)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "listactions" /: test =>
              try {
                val status = (scheduleActor ? GetStatus()).mapTo[ProcessList]
                status.map(pl => {
                   val resp = s"{" +
                    s""" "running" :  ${pl.status.filter { case s: HiveStatusResponse => s.status == RUNNING; case s: OozieStatusResponse => s.status == RUNNING }.toList.size},""" +
                    s""" "idle" : ${pl.status.filter { case s: HiveStatusResponse => s.status == IDLE; case s: OozieStatusResponse => s.status == IDLE }.toList.size},""" +
                    s""" "processes" : [ ${
                      pl.status.map(a => {
                        a match {
                          case s: HiveStatusResponse =>  s"""{"status":"${s.message}", "typ":"hive", "start":"${formatter.print(s.start)}", "query":"${new String(enc.quoteAsString(s.query))}"} """
                          case s: OozieStatusResponse => s"""{"status":"${s.message}", "typ":"oozie", "start":"${formatter.print(s.start)}", "jobId":"${s.jobId}"} """
                        }                        
                      }).mkString(",")}]}
                     """
                  sendOk(request, resp)
                  })                  
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }
            case request @ Get on Root /: "listviews" /: state =>
              try {
                val gatherActor = settings.system.actorOf(Props(new ViewStatusRetriever()))
                val status = (gatherActor ? GetStatus()).mapTo[List[ViewStatusResponse]]
                status.map( views => {
                    val stats = views.groupBy(_.state)
                                     .mapValues(_.size)
                                     .map(a => s""""${a._1}" : ${a._2},""")
                                     .mkString("")
                    val filtered = views.filter("any".equals(state) || _.state.equals(state))
                    val details = filtered.map(v => s"""{"status":"${v.state}", "view":"${v.view.n}", "parameters":"${v.view.partitionSpec}"}""").mkString(",")
                    sendOk(request, s"""{ ${stats}  "details" : [ ${details} ] }""")
                })                                 
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }
            case request @ Get on Root /: "kill" /: id =>
              try {
                val result = (settings.system.actorFor(id) ? KillAction)

                result.map { case InternalError(s) => request.error(s, headers) case _ => request.ok("ok", headers) }
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "resume" /: id =>
              try {
                request.ok("ok", headers)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }
          }
        })
    }
  }

  def main(args: Array[String]) {
    start()
  }

  private def deploy() {
    scheduleActor ! Deploy()
  }

  private def getViewActors(viewUrlPath: String) = {
    val views = if (viewAugmentor != null)
      View.viewsFromUrl(viewUrlPath, viewAugmentor)
    else
      View.viewsFromUrl(viewUrlPath)

    val viewActorRefFutures = views.map { v => (supervisor ? v).mapTo[ActorRef] }
    Await.result(Future sequence viewActorRefFutures, 60 seconds)
  }

  private def errorResponseWithStacktrace(request: colossus.protocols.http.Http#Input, t: Throwable) = {
    t.printStackTrace()
    request.error(t.getStackTrace().foldLeft("")((s, e) => s + e.toString() + "\n"),
      List(("content-type", "text/plain")))
  }
    
  private def formatJson(json: String) = {    
    om.defaultPrettyPrintingWriter().writeValueAsString(om.readValue(json, classOf[Object]))    
  }
  
  private def sendOk(r: HttpRequest, s: String) = {
    try {
      r.ok(formatJson(s), headers)
    } catch {
      case j: Exception => r.error("Cannot parse JSON: " + s, List(("Content-type", "text/plain")) )
    }
    
  }
  
  private def sendError(r: HttpRequest, s: String) = {
    try {
      r.error(formatJson(s), headers)
    } catch {
      case j: Exception => r.error("Cannot parse JSON: " + s, List(("Content-type", "text/plain")) )
    }    
  }  
    
}
