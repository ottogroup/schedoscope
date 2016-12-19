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
package org.schedoscope.scheduler.service

import java.util.regex.Pattern

import akka.actor.{ActorRef, ActorSystem, actorRef2Scala}
import akka.event.Logging
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import org.schedoscope.AskPattern._
import org.schedoscope.conf.SchedoscopeSettings
import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations._
import org.schedoscope.scheduler.actors.ViewManagerActor
import org.schedoscope.scheduler.driver.{DriverRunFailed, DriverRunOngoing, DriverRunState, DriverRunSucceeded}
import org.schedoscope.scheduler.messages._
import org.schedoscope.schema.ddl.HiveQl

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps


class SchedoscopeServiceImpl(actorSystem: ActorSystem, settings: SchedoscopeSettings, viewManagerActor: ActorRef, transformationManagerActor: ActorRef) extends SchedoscopeService {

  val log = Logging(actorSystem, classOf[ViewManagerActor])

  transformationManagerActor ! DeployCommand()

  private def getOrElse[T](o: T, d: T) = if (o != null) o else d

  private def checkFilter(filter: Option[String]) {
    if (filter.isDefined)
      try {
        Pattern.compile(filter.get)
      } catch {
        case t: Throwable => throw new IllegalArgumentException(s"Invalid regular expression passed: ${filter.get}.", t)
      }
  }

  private def checkViewUrlPath(viewUrlPath: Option[String]) {
    if (viewUrlPath.isDefined && !viewUrlPath.get.isEmpty)
      try {
        val views = viewsFromUrl(viewUrlPath.get)
        //
        // Block access to external views
        //
        if (settings.externalDependencies) {
          views.foreach { v =>
            if (!v.isInDatabases(settings.externalHome: _*))
              throw new UnsupportedOperationException("You can not address an external view directly.")
          }
        }
      } catch {
        case t: Throwable => throw new IllegalArgumentException(s"Invalid view URL pattern passed: ${viewUrlPath.get}."
          + {
          if (t.getMessage != null) s"\noriginal Message: ${t.getMessage}"
        }, t)
      }
  }

  private def viewsFromUrl(viewUrlPath: String) =
    View.viewsFromUrl(settings.env, viewUrlPath, settings.viewAugmentor)

  private def retrieveViewStatus(viewUrlPath: Option[String],
                            status: Option[String],
                            filter: Option[String],
                            issueFilter: Option[String],
                            dependencies: Boolean = false) = {
    val cf = Future(checkFilter(filter))
    val cvup = Future(checkViewUrlPath(viewUrlPath))

    Future.sequence(List(cf, cvup)).flatMap { r =>
      Future {
        val resolvedViews = if (viewUrlPath.isDefined && !viewUrlPath.get.isEmpty) Some(viewsFromUrl(viewUrlPath.get)) else None
        queryActor[ViewStatusListResponse](
          viewManagerActor,
          GetViews(resolvedViews, status, filter, issueFilter, dependencies),
          settings.schedulingCommandTimeout).viewStatusList
      }
    }
  }

  /**
    * Convenience method for DRYing viewStatusListFromStatusResponses
    */
  private def buildViewStatusOutput(vsr: ViewStatusResponse,
                                    viewTableName: Option[String],
                                    isTable: Option[Boolean],
                                    dependencies: Option[Boolean],
                                    overview: Boolean = true,
                                    all: Option[Boolean],
                                    includeProps: Boolean
                                   ) = {
    val properties =
      if (includeProps)
        Some(Map("errors" -> vsr.errors.getOrElse(false).toString,
          "incomplete" -> vsr.incomplete.getOrElse(false).toString))
      else
        None

    ViewStatus(
      viewPath = vsr.view.urlPath,
      viewTableName = viewTableName,
      status = vsr.status,
      properties = properties,
      fields = if (overview) None else Option(vsr.view.fields.map(f => FieldStatus(f.n, HiveQl.typeDdl(f.t), f.comment)).toList),
      parameters = if (overview || vsr.view.parameters.isEmpty) None
      else
        Some(vsr.view.parameters.map(p => FieldStatus(p.n, p.t.runtimeClass.getSimpleName, None)).toList),
      dependencies = if ((dependencies.getOrElse(false) || all.getOrElse(false)) && vsr.view.dependencies.nonEmpty)
        Some(vsr.view.dependencies.map(d => (d.tableName, d.urlPath)).groupBy(_._1).mapValues(_.toList.map(_._2)))
      else
        None,
      transformation = if (overview) None else Option(vsr.view.registeredTransformation().viewTransformationStatus),
      export = if (overview) None else Option(viewExportStatus(vsr.view.registeredExports.map(e => e.apply()))),
      storageFormat = if (overview) None else Option(vsr.view.storageFormat.getClass.getSimpleName),
      materializeOnce = if (overview) None else Option(vsr.view.isMaterializeOnce),
      comment = if (overview) None else Option(vsr.view.comment),
      isTable = isTable
    )

  }

  private def matchFinalStatus(vsr: ViewStatusResponse) =
    vsr.status match {
      case "materialized" => true
      case "failed" => true
      case _ => false
    }

  private def viewStatusListFromStatusResponses(viewStatusResponses: List[ViewStatusResponse],
                                                dependencies: Option[Boolean],
                                                overview: Option[Boolean],
                                                all: Option[Boolean],
                                                issueFilter: Option[String] = None
                                               ) = {

    val viewStatusListWithoutViewDetails = viewStatusResponses.map { v =>
      buildViewStatusOutput(vsr = v,
        viewTableName = if (all.getOrElse(false)) Some(v.view.tableName) else None,
        isTable = if (all.getOrElse(false)) Some(false) else None,
        dependencies = dependencies,
        overview = true,
        all = all,
        includeProps = matchFinalStatus(v) || issueFilter.isDefined
      )
    }

    lazy val viewStatusList = if (all.getOrElse(false))
      viewStatusResponses
        .groupBy(v => v.view.tableName)
        .map(e => e._2.head)
        .map(v =>
          buildViewStatusOutput(vsr = v,
            viewTableName = Option(v.view.tableName),
            isTable = Option(true),
            dependencies = dependencies,
            overview = false,
            all = all,
            includeProps = matchFinalStatus(v) || issueFilter.isDefined
          )
        )
        .toList ::: viewStatusListWithoutViewDetails
    else
      viewStatusListWithoutViewDetails

    val statusOverview = viewStatusListWithoutViewDetails.groupBy(_.status).mapValues(_.size)

    ViewStatusList(statusOverview, if (overview.getOrElse(false)) List() else viewStatusList)
  }

  private def formatDate(d: LocalDateTime): String =
    if (d != null) DateTimeFormat.shortDateTime().print(d) else ""

  private def parseActionStatus(a: TransformationStatusResponse[_]): TransformationStatus = {
    val actor = getOrElse(a.actor.path.toStringWithoutAddress, "unknown")
    val typ = if (a.driver != null) getOrElse(a.driver.transformationName, "unknown") else "unknown"
    var drh = a.driverRunHandle
    var status = if (a.message != null) a.message else "none"
    var comment = ""

    if (a.driverRunStatus != null) {
      a.driverRunStatus.asInstanceOf[DriverRunState[Any with Transformation]] match {

        case s: DriverRunSucceeded[_] =>
          comment = getOrElse(s.comment, "no-comment")
          status = "succeeded"


        case f: DriverRunFailed[_] =>
          comment = getOrElse(f.reason, "no-reason")
          status = "failed"


        case o: DriverRunOngoing[_] =>
          drh = o.runHandle

      }
    }

    if (drh != null) {
      val desc = drh.transformation.asInstanceOf[Transformation].description
      val view = drh.transformation.asInstanceOf[Transformation].getView()
      val started = drh.started
      val runStatus = RunStatus(getOrElse(desc, "no-desc"), getOrElse(view, "no-view"), getOrElse(formatDate(started), ""), comment, None)

      TransformationStatus(actor, typ, status, Some(runStatus), None)
    } else
      TransformationStatus(actor, typ, status, None, None)

  }

  private def parseQueueElements(q: List[AnyRef]): List[RunStatus] = q.map {
    case trans: Transformation => RunStatus(trans.description, trans.getView(), "", "", None)
    case other => RunStatus(other.toString, "", "", "", None)
  }


  private def viewExportStatus(exports: List[Transformation]): List[ViewTransformationStatus] = {
    exports.map(e =>
      if (e.configuration.contains("schedoscope.export.jdbcConnection")) {
        ViewTransformationStatus("JDBC", Some(Map(
          "JDBC Url" -> e.configuration.get("schedoscope.export.jdbcConnection").get.toString,
          "User" -> e.configuration.get("schedoscope.export.dbUser").get.toString,
          "Storage Engine" -> e.configuration.get("schedoscope.export.storageEngine").get.toString,
          "Reducers" -> e.configuration.get("schedoscope.export.numReducers").get.toString,
          "Batch Size" -> e.configuration.get("schedoscope.export.commitSize").get.toString)))
      } else if (e.configuration.contains("schedoscope.export.redisHost")) {
        ViewTransformationStatus("Redis", Some(Map(
          "Host" -> e.configuration.get("schedoscope.export.redisHost").get.toString,
          "Port" -> e.configuration.get("schedoscope.export.redisPort").get.toString,
          "Key Space" -> e.configuration.get("schedoscope.export.redisKeySpace").get.toString,
          "Reducers" -> e.configuration.get("schedoscope.export.numReducers").get.toString,
          "Pipeline" -> e.configuration.get("schedoscope.export.pipeline").get.toString)))
      } else if (e.configuration.contains("schedoscope.export.kafkaHosts")) {
        ViewTransformationStatus("Kafka", Some(Map(
          "Hosts" -> e.configuration.get("schedoscope.export.kafkaHosts").get.toString,
          "Zookeeper" -> e.configuration.get("schedoscope.export.zookeeperHosts").get.toString,
          "Partitions" -> e.configuration.get("schedoscope.export.numPartitions").get.toString,
          "Replication Factor" -> e.configuration.get("schedoscope.export.replicationFactor").get.toString,
          "Reducers" -> e.configuration.get("schedoscope.export.numReducers").get.toString)))
      } else {
        ViewTransformationStatus(e.name, None)
      })
  }

  def materialize(viewUrlPath: Option[String], status: Option[String], filter: Option[String], issueFilter: Option[String], mode: Option[String]) = {
    retrieveViewStatus(viewUrlPath, status, filter, issueFilter).map {
      viewStatusResponses =>
        viewStatusResponses
          .foreach { vsr =>
            vsr.actor ! MaterializeView(
              try {
                MaterializeViewMode.withName(mode.getOrElse("DEFAULT"))
              } catch {
                case _: NoSuchElementException => MaterializeViewMode.DEFAULT
              })
          }
        viewStatusListFromStatusResponses(viewStatusResponses, None, None, None, issueFilter)
    }
  }

  def invalidate(viewUrlPath: Option[String], status: Option[String], filter: Option[String], issueFilter: Option[String], dependencies: Option[Boolean]) = {
    retrieveViewStatus(viewUrlPath, status, filter, issueFilter, dependencies.getOrElse(false)).map {
      viewStatusResponses =>
        viewStatusResponses
          .foreach { vsr =>
            vsr.actor ! InvalidateView()
          }
        viewStatusListFromStatusResponses(viewStatusResponses, dependencies, None, None, issueFilter)
    }
  }

  def newdata(viewUrlPath: Option[String], status: Option[String], filter: Option[String]) = {
    retrieveViewStatus(viewUrlPath, status, filter, None).map { viewStatusResponses =>
      viewStatusResponses
        .foreach { vsr =>
          vsr.actor ! "newdata"
        }
      viewStatusListFromStatusResponses(viewStatusResponses, None, None, None, None)
    }
  }

  def views(viewUrlPath: Option[String], status: Option[String], filter: Option[String], issueFilter: Option[String], dependencies: Option[Boolean], overview: Option[Boolean], all: Option[Boolean]) =
    retrieveViewStatus(viewUrlPath, status, filter, issueFilter, dependencies.getOrElse(false)).map { viewStatusResponses =>
      viewStatusListFromStatusResponses(viewStatusResponses, dependencies, overview, all, issueFilter)
    }

  def transformations(status: Option[String], filter: Option[String]): Future[TransformationStatusList] = {
    val cf = Future(checkFilter(filter))
    cf.flatMap { r =>
      Future {
        val result = queryActor[TransformationStatusListResponse](
          transformationManagerActor, GetTransformations(), settings.schedulingCommandTimeout)
        val actions = result.transformationStatusList
          .map(a => parseActionStatus(a))
          .filter(a => status.isEmpty || status.get.equals(a.status))
          .filter(a => filter.isEmpty || a.actor.matches(filter.get))
        // FIXME: is the actor name a good filter criterion?
        val overview = actions
          .groupBy(_.status)
          .map(el => (el._1, el._2.size))

        TransformationStatusList(overview, actions)
      }
    }
  }

  def queues(typ: Option[String], filter: Option[String]): Future[QueueStatusList] = {
    val cf = Future(checkFilter(filter))
    cf.flatMap { r =>
      Future {
        val result = queryActor[QueueStatusListResponse](
          transformationManagerActor, GetQueues(), settings.schedulingCommandTimeout)

        val queues = result.transformationQueues
          .filterKeys(t => typ.isEmpty || t.startsWith(typ.get))
          .map { case (t, queue) => (t, parseQueueElements(queue)) }
          .map { case (t, queue) => (t, queue.filter(el => filter.isEmpty || el.targetView.matches(filter.get))) }
        val overview = queues
          .map(el => (el._1, el._2.size))
        QueueStatusList(overview, queues)
      }
    }
  }

  def shutdown(): Boolean = {
    actorSystem.shutdown()
    actorSystem.awaitTermination(5 seconds)
    actorSystem.isTerminated
  }
}
