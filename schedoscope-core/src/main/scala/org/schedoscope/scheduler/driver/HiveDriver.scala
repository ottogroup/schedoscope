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
package org.schedoscope.scheduler.driver


import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.session.SessionState
import org.joda.time.LocalDateTime
import org.schedoscope.Schedoscope
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.Transformation.replaceParameters
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.Stack
import scala.concurrent.Future

/**
  * Driver for executing Hive transformations
  */
class HiveDriver(val driverRunCompletionHandlerClassNames: List[String], val conf: HiveConf, val metastoreClient: HiveMetaStoreClient) extends DriverOnBlockingApi[HiveTransformation] {

  def transformationName = "hive"

  val log = LoggerFactory.getLogger(classOf[HiveDriver])

  /**
    * Construct a future-based driver run handle
    */
  def run(t: HiveTransformation): DriverRunHandle[HiveTransformation] =
    new DriverRunHandle[HiveTransformation](this, new LocalDateTime(), t, Future {

      t.udfs.foreach(this.registerFunction(_))

      executeHiveQuery(replaceParameters(t.sql, t.configuration.toMap))

    })

  /**
    * Register UDFs with Metastore.
    */
  def registerFunction(f: Function) {

    val existing = try {
      metastoreClient.getFunctions(f.getDbName, f.getFunctionName)
    } catch {
      case t: Throwable => throw RetryableDriverException(s"Runtime exception while executing registering function ${f}", t)
    }

    if (existing == null || existing.isEmpty()) {
      val resourceJars = f.getResourceUris.map(jar => s"JAR '${jar.getUri}'").mkString(", ")
      val createFunction = s"CREATE FUNCTION ${f.getDbName}.${f.getFunctionName} AS '${f.getClassName}' USING ${resourceJars}"

      this.executeHiveQuery(createFunction)
    }
  }

  /**
    * Actually perform the given query and return a run state after completion.
    */
  def executeHiveQuery(sql: String): DriverRunState[HiveTransformation] = {

    SessionState.start(conf)

    try {

      for (q <- splitQueryIntoStatements(sql)) {

        val result = CommandProcessorFactory.get(q.trim.split("\\s+"), conf).run(q)

        if (result.getResponseCode != 0)
          return DriverRunFailed(this, s"Hive returned error while executing Hive query ${q}. Response code: ${result.getResponseCode} SQL State: ${result.getSQLState}, Error message: ${result.getErrorMessage}", result.getException)

      }

    } catch {
      case t: Throwable =>
        return DriverRunFailed[HiveTransformation](this, s"Unknown exception caught while executing Hive query ${sql}. Failing run.", t)
    } finally {
        SessionState.detachSession()
    }

    DriverRunSucceeded[HiveTransformation](this, s"Hive query ${sql} executed")
  }

  private def splitQueryIntoStatements(sql: String) = {
    val queryStack = Stack[String]("")

    sql
      .split(";")
      .foreach { statement => {
        if (StringUtils.endsWith(queryStack.head, "\\")) {
          queryStack.push(StringUtils.chop(queryStack.pop()) + ";" + statement)
        } else {
          queryStack.push(statement)
        }
      }
      }

    queryStack.reverse.filter {
      !StringUtils.isBlank(_)
    }
  }
}

/**
  * Factory methods for Hive drivers
  */
object HiveDriver {

  def apply(ds: DriverSettings) = {
    val ugi = Schedoscope.settings.userGroupInformation

    val conf = new HiveConf()
    conf.set("hive.metastore.local", "false");
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, Schedoscope.settings.metastoreUri.trim());

    if (Schedoscope.settings.kerberosPrincipal.trim() != "") {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true);
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        Schedoscope.settings.kerberosPrincipal);
    }

    val metastoreClient = new HiveMetaStoreClient(conf)

    new HiveDriver(ds.driverRunCompletionHandlers, conf, metastoreClient)
  }
}

