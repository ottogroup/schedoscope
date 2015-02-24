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
import com.ottogroup.bi.soda.bottler.NoDataAvaiable
import com.ottogroup.bi.soda.bottler.NewDataAvailable
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
import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import akka.actor.ActorRef

object SodaService {
  val settings = Settings()

  implicit val io = IOSystem()
  implicit val ec = ExecutionContext.global
  implicit val timeout = Timeout(3 days) // needed for `?` below

  val supervisor = settings.system.actorOf(ViewSuperVisor.props(settings.userGroupInformation, settings.hadoopConf), "supervisor")
  val scheduleActor = settings.system.actorOf(ActionsRouterActor.props(settings.hadoopConf), "actions")
  val schemaActor = settings.system.actorOf(SchemaActor.props(settings.jdbcUrl, settings.metastoreUri, settings.kerberosPrincipal), "schemaActor")

  val viewAugmentor = if (settings.parsedViewAugmentorClass != "")
    Class.forName(settings.parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]
  else
    null
  val formatter = DateTimeFormat.fullDateTime()

  def start() {
    deploy()
    Service.serve[Http]("http-service", settings.port, settings.webserviceTimeOut) {
      (context =>
        context.handle { connection =>
          connection.become {
            case request @ Get on Root => request.ok("Health check OK")

            case request @ Get on Root /: "materialize" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => viewActor ? "materialize")
                val res = Await.result(Future sequence fut, 1 hour)
                val result = res.foldLeft(0) { (count, r) =>
                  r match {
                    case ViewMaterialized(v, incomplete, changed) => count + 1

                    case _: NoDataAvaiable => count
                  }
                }
                if (result == res.size)
                  request.ok(s"""{ \n"status":"success"\n"view":"${viewName(viewUrlPath)}\n}"""")
                else if (result == 0)
                  request.ok(s"""{ \n"status":"nodata"\n"view":"${viewName(viewUrlPath)}\n}"""")
                else
                  request.ok(s"""{ \n"status":"incomplete"\n"view":"${viewName(viewUrlPath)}\n}"""")
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "status" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? "status").mapTo[ViewStatus])
                val res = Await.result(Future sequence fut, 1 hour)
                val resultMessage = res.foldLeft("") { (message, current) => message + current.view.toString() + "\n" }
                request.ok(resultMessage)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "invalidate" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? "invalidate").mapTo[View])
                val res = Await.result(Future sequence fut, 1 hour)
                val resultMessage = res.foldLeft("") { (message, current) => message + current.toString() + "\n" }
                request.ok(resultMessage)
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "newdata" /: viewUrlPath =>
              try {
                val viewActors = getViewActors(viewUrlPath)
                val fut = viewActors.map(viewActor => (viewActor ? "status").mapTo[ViewStatus])
                val viewActorsStatus = Await.result(Future sequence fut, 1 hour)
                viewActors.zip(viewActorsStatus).foreach { case (a, s) => a ! NewDataAvailable(s.view) }
                request.ok("ok")
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "listactions" /: test =>
              try {
                val status = (scheduleActor ? GetStatus()).mapTo[ProcessList]
                status.map(pl => request.ok(s"{" +
                  s"""running:  ${pl.status.filter { case s: HiveStatusResponse => s.status == RUNNING; case s: OozieStatusResponse => s.status == RUNNING }.toList.size},\n""" +
                  s"""idle: ${pl.status.filter { case s: HiveStatusResponse => s.status == IDLE; case s: OozieStatusResponse => s.status == IDLE }.toList.size},\n""" +
                  s"""processes :[ ${
                    pl.status.foldLeft("") {
                      case (json: String, s: HiveStatusResponse) => json + s"""{status:"${s.message}"\ntyp:"hive"\nstart:"${formatter.print(s.start)}\nquery:"${s.query}"}\n""""
                      case (json: String, s: OozieStatusResponse) => json + s"""{status:"${s.message}"\ntyp:"oozie"\nstart:"${formatter.print(s.start)}\njobId:"${s.jobId}"}\n""""
                    }
                  } ]""" +
                  "\n}"))

              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "kill" /: id =>
              try {
                val result = (settings.system.actorFor(id) ? KillAction)

                result.map { case InternalError(s) => request.error(s) case _ => request.ok("ok") }
              } catch {
                case t: Throwable => errorResponseWithStacktrace(request, t)
              }

            case request @ Get on Root /: "resume" /: id =>
              try {
                request.ok("ok")
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
    Await.result(Future sequence viewActorRefFutures, 1000 milliseconds)
  }

  private def errorResponseWithStacktrace(request: colossus.protocols.http.Http#Input, t: Throwable) = {
    t.printStackTrace()
    request.error(t.getStackTrace().foldLeft("")((s, e) => s + e.toString() + "\n"),
      List(("content-type", "text/plain")))
  }
}
