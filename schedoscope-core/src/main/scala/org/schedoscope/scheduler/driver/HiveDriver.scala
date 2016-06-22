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


import java.io.{OutputStream, PrintStream}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Function
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
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
class HiveDriver(val driverRunCompletionHandlerClassNames: List[String], val conf: HiveConf) extends DriverOnBlockingApi[HiveTransformation] {

  def transformationName = "hive"

  val log = LoggerFactory.getLogger(classOf[HiveDriver])

  /**
    * Construct a future-based driver run handle
    */
  def run(t: HiveTransformation): DriverRunHandle[HiveTransformation] =
    new DriverRunHandle[HiveTransformation](this, new LocalDateTime(), t, Future {
      executeHiveQuery(t.udfs, replaceParameters(t.sql, t.configuration.toMap))
    })

  /**
    * Actually perform the given query - after registering required functions - and return a run state after completion.
    */
  def executeHiveQuery(functionsToRegister: List[Function], sql: String): DriverRunState[HiveTransformation] = {

    //
    // Setup session state
    //

    try {
      setupSessionState()
    } catch {
      case t: Throwable =>

        //
        // Could not create session state but we should retry
        //
        // Cleanup
        //

        tearDownSessionState()

        throw RetryableDriverException(s"Cannot create session state for query ${sql}", t)
    }

    //
    // Enhance sql with any necessary create function statements
    //

    val sqlPlusCreateFunctions = functionsToRegister
      .foldLeft(sql) {

        case (sqlSoFar, f) =>

          val existing = try {
            Hive.get().getMSC.getFunctions(f.getDbName, f.getFunctionName)
          } catch {
            case t: Throwable =>

              //
              // Could not query Metastore for function, but we should retry
              //
              // Cleanup session state
              //

              tearDownSessionState()

              throw RetryableDriverException(s"Runtime exception while querying registered function ${f}", t)
          }

          if (existing == null || existing.isEmpty)
            s"CREATE FUNCTION ${f.getDbName}.${f.getFunctionName} AS '${f.getClassName}' USING ${f.getResourceUris.map(jar => s"JAR '${jar.getUri}'").mkString(", ")}; ${sqlSoFar}"
          else
            sqlSoFar
      }

    //
    // Execute query statements one by one
    //

    try {
      splitQueryIntoStatements(sqlPlusCreateFunctions)
        .foldLeft[DriverRunState[HiveTransformation]](
        DriverRunSucceeded[HiveTransformation](this, s"Hive query ${sql} executed")
      ) {

        case (noProblemSoFar: DriverRunSucceeded[HiveTransformation], statement) =>

          val commandTokens = statement.trim.split("\\s+")
          val commandType = commandTokens(0)
          val remainingStatement = statement.trim.substring(commandType.length)

          val result = CommandProcessorFactory.get(commandType) match {
            case statementDriver: org.apache.hadoop.hive.ql.Driver => statementDriver.run(statement)

            case otherProcessor: CommandProcessor => otherProcessor.run(remainingStatement)
          }


          if (result.getResponseCode != 0) {

            //
            // Unrecoverable error, we need to fail.
            //

            DriverRunFailed(this, s"Hive returned error while executing Hive query ${statement}. Response code: ${result.getResponseCode} SQL State: ${result.getSQLState}, Error message: ${result.getErrorMessage}", result.getException)
          }
          else
            noProblemSoFar


        case (failure, _) => failure
      }
    } catch {
      case t: Throwable =>

        //
        // Unrecoverable error, we need to fail.
        //

        return DriverRunFailed[HiveTransformation](this, s"Unknown exception caught while executing Hive query ${sql}. Failing run.", t)

    } finally {

      //
      // Cleanup session state in any case
      //

      tearDownSessionState()
    }
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

  private def setupSessionState() {
    SessionState.start(new HiveConf(conf))
    SessionState.get().out = muton
    SessionState.get().err = muton
    SessionState.get().info = muton
  }


  private def tearDownSessionState() {
    try {
      Hive.closeCurrent()
    } catch {
      case _: Throwable =>
    }

    try {
      SessionState.get().close()
    } catch {
      case _: Throwable =>
    }
  }

  private def muton = new PrintStream(new OutputStream {
    override def write(b: Int): Unit = {}
  })
}

/**
  * Factory methods for Hive drivers
  */
object HiveDriver {

  def apply(ds: DriverSettings) = {
    val ugi = Schedoscope.settings.userGroupInformation

    val conf = new HiveConf(classOf[SessionState])

    conf.set("hive.metastore.local", "false")
    conf.set("hive.auto.convert.join", "false")
    conf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, true)
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, Schedoscope.settings.metastoreUri.trim())

    if (Schedoscope.settings.kerberosPrincipal.trim() != "") {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true)
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        Schedoscope.settings.kerberosPrincipal)
    }

    new HiveDriver(ds.driverRunCompletionHandlers, conf)
  }
}

