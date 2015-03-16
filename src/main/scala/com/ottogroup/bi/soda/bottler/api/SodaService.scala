package com.ottogroup.bi.soda.bottler.api

import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import org.codehaus.jackson.map.ObjectMapper
import org.joda.time.format.DateTimeFormat
import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.ottogroup.bi.soda.bottler.ActionsManagerActor
import com.ottogroup.bi.soda.bottler.Deploy
import com.ottogroup.bi.soda.bottler.Failed
import com.ottogroup.bi.soda.bottler.GetStatus
import com.ottogroup.bi.soda.bottler.InternalError
import com.ottogroup.bi.soda.bottler.KillAction
import com.ottogroup.bi.soda.bottler.NewDataAvailable
import com.ottogroup.bi.soda.bottler.NoDataAvailable
import com.ottogroup.bi.soda.bottler.SchemaActor
import com.ottogroup.bi.soda.bottler.ViewManagerActor
import com.ottogroup.bi.soda.bottler.ViewMaterialized
import com.ottogroup.bi.soda.bottler.ViewStatus
import com.ottogroup.bi.soda.bottler.ViewStatusResponse
import com.ottogroup.bi.soda.bottler.ViewStatusRetriever
import com.ottogroup.bi.soda.bottler.driver.DriverRunOngoing
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.ParsedViewAugmentor
import com.ottogroup.bi.soda.dsl.views.ViewUrlParser.viewNames
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import colossus.IOSystem
import colossus.protocols.http.Http
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.HttpProvider
import colossus.protocols.http.HttpRequest
import colossus.protocols.http.UrlParsing.Strings
import colossus.protocols.http.UrlParsing.Strings._
import colossus.service.Response.liftCompletedFuture
import colossus.service.Response.liftCompletedSync
import colossus.service.Service
import com.ottogroup.bi.soda.bottler.ActionStatusListResponse
import com.ottogroup.bi.soda.bottler.ViewStatusListResponse

object SodaService {
  val settings = Settings()

  val om = new ObjectMapper()
  val enc = JsonStringEncoder.getInstance

  val headers = List(("Content-Type", "application/json"), ("Access-Control-Allow-Origin", "*"))

  implicit val io = IOSystem()
  implicit val ec = ExecutionContext.global
  implicit val timeout = Timeout(3 days) // needed for `?` below

  val viewManagerActor = settings.system.actorOf(ViewManagerActor.props(settings), "views")
  val actionsManagerActor = settings.system.actorOf(ActionsManagerActor.props(settings.hadoopConf), "actions")
  val schemaActor = settings.system.actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "schema")

  val viewAugmentor = if (settings.parsedViewAugmentorClass != "")
    Class.forName(settings.parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]
  else
    null
  val formatter = DateTimeFormat.shortDateTime()

  def start() {
    deploy()
    Service.serve[Http]("http-service", settings.port, settings.webserviceTimeOut) {
      (context =>
        context.handle { connection =>
          connection.become {
            case request @ Get on Root => sendOk(request, """{"status" : "ok"}""")

            case request @ Get on Root /: "materialize" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => viewActor ? "materialize")
                val res = Await.result(Future sequence fut, 10 days)
                val successCount = res.foldLeft(0) { (count, r) =>
                  r match {
                    case ViewMaterialized(view, incomplete, changed, errors) => count + 1
                    case NoDataAvailable(view) => count
                    case Failed(view) => count
                  }
                }
                if (successCount == res.size)
                  sendOk(request, s"""{ "status":"success", "views":"${viewNames(viewUrlPath).mkString(",")}"}""")
                else if (successCount == 0)
                  sendOk(request, s"""{ "status":"nodata", "views":"${viewNames(viewUrlPath).mkString(",")}"}""")
                else
                  sendOk(request, s"""{ "status":"incomplete", "views":"${viewNames(viewUrlPath).mkString(",")}"}""")
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "status" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? GetStatus()).mapTo[ViewStatusResponse])
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
                val fut = viewActors.map(viewActor => (viewActor ? GetStatus()).mapTo[ViewStatusResponse])
                val viewActorsStatus = Await.result(Future sequence fut, 1 hour)
                viewActors.zip(viewActorsStatus).foreach { case (a, s) => a ! NewDataAvailable(s.view) }
                request.ok("ok", headers)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "listactions" /: test =>
              try {
                val status = (actionsManagerActor ? GetStatus()).mapTo[ActionStatusListResponse]
                status.map(actionStatusResponses => {
                  val resp = s"""{
                     "running" : ${actionStatusResponses.actionStatusList.filter { _.driverRunStatus.isInstanceOf[DriverRunOngoing[_]] }.size},
                     "idle" : ${actionStatusResponses.actionStatusList.filter { _.driverRunHandle == null }.size},
                     "queued" : ${actionStatusResponses.actionQueueStatus.foldLeft(0)((s, el) => s + el._2.size)},  
                     "queues" : { ${actionStatusResponses.actionQueueStatus.map(ql => s""" "${ql._1}" : [ ${ql._2.map(el => "\"" + new String(enc.quoteAsString(el)) + "\"").mkString(",")} ] """).mkString(",")} },  
                     "processes" : [ 
                     	${actionStatusResponses.actionStatusList.map { s => s"""{"status":"${s.message}", "typ":"${s.driver.transformationName}", "start":"${if (s.driverRunHandle != null) formatter.print(s.driverRunHandle.started) else ""}", "transformation":"${if (s.driverRunHandle != null) new String(enc.quoteAsString(s.driverRunHandle.transformation.asInstanceOf[Transformation].description)) else ""}"}""" }.mkString(",")}
                     ]}"""
                  sendOk(request, resp)
                })
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "listviews" /: state =>
              try {
                val status = (viewManagerActor ? GetStatus()).mapTo[ViewStatusListResponse]
                status.map(viewStatusResponses => {
                  val views = viewStatusResponses.viewStatusList
                  val stats = views.groupBy(_.status)
                    .mapValues(_.size)
                    .map(a => s""""${a._1}" : ${a._2}""")
                    .mkString(",")
                  val filtered = views.filter("any".equals(state) || _.status.equals(state))
                  val details = filtered.map(v => s"""{"status":"${v.status}", "view":"${v.view.n}", "parameters":"${v.view.partitionSpec}"}""").mkString(",")
                  sendOk(request, s"""{ "overview" : { ${stats} }, "details" : [ ${details} ] }""")
                })
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "dependencygraph" /: dummy =>
              try {
                val status = (viewManagerActor ? GetStatus()).mapTo[ViewStatusListResponse]
                val nodes = HashSet[(String, String)]()
                val edges = HashSet[(String, String)]()
                val colors = Map(("materialized", "lime"), ("transforming", "yellow"), ("nodata", "beige"), ("table", "black"), ("failed", "red"), ("retrying", "orange"), ("receive", "powderblue"), ("waiting", "blue"), ("dummy", "white"))
                status.map(viewStatusResponses => {
                  val views = viewStatusResponses.viewStatusList
                  views.foreach(v => {
                    if (v.status != "receive" && v.status != "nodata") {
                      //if (true) {
                      nodes.add((v.view.viewId, v.status))
                      v.view.dependencies.foreach(d => {
                        edges.add((d.viewId, v.view.viewId))
                      })
                    }
                  })
                  val outer = nodes.map(n => n._1).toSeq.diff(edges.map(e => e._1).toSeq.distinct)
                  outer.foreach(o => {
                    val tab = o.split("/")(0)
                    val db = tab.split("\\.")(0)
                    nodes.add((tab, "table"))
                    edges.add((o, tab))
                    edges.add((tab, "ROOT"))
                  })
                  nodes.add(("ROOT", "dummy"))
                  val nodeList = nodes.toList.zipWithIndex.map(el => (el._1._1, (el._2, el._1._2)))
                  val nodeLookup = nodeList.map(nl => (nl._1, nl._2._1)).toMap

                  val nodeListJson = nodeList.map(n => s"""{"name":"${n._1} : ${n._2._2}", "color":"${colors.get(n._2._2).get}"}""").mkString(",")
                  val edgeListJson = edges.filter(e => { nodeLookup.contains(e._1) && nodeLookup.contains(e._2) })
                    .map(e => s"""{"source":${nodeLookup.get(e._1).get}, "target":${nodeLookup.get(e._2).get}, "value":1}""")
                    .mkString(",")

                  sendOk(request, s"""{"rootId": ${nodeLookup.get("ROOT").get}, "nodes" : [${nodeListJson}], "links" : [${edgeListJson}] }""")
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
    actionsManagerActor ! Deploy()
  }

  private def getViewActors(viewUrlPath: String) = {
    val views = if (viewAugmentor != null)
      View.viewsFromUrl(Settings().env, viewUrlPath, viewAugmentor)
    else
      View.viewsFromUrl(Settings().env, viewUrlPath)

    println("COMPUTED VIEWS: " + views.map(v => v.viewId).mkString("\n"))

    val viewActorRefFutures = views.map { v => (viewManagerActor ? v).mapTo[ActorRef] }
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
      case j: Exception => r.error("Cannot parse JSON: " + s, List(("Content-type", "text/plain")))
    }

  }

  private def sendError(r: HttpRequest, s: String) = {
    try {
      r.error(formatJson(s), headers)
    } catch {
      case j: Exception => r.error("Cannot parse JSON: " + s, List(("Content-type", "text/plain")))
    }
  }

}
