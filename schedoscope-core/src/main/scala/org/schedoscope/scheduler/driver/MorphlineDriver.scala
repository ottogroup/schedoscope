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

import java.net.URI
import java.security.PrivilegedAction
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.seqAsJavaList
import scala.concurrent.future
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDateTime
import org.kitesdk.morphline.api.Command
import org.kitesdk.morphline.api.MorphlineContext
import org.kitesdk.morphline.api.Record
import org.kitesdk.morphline.avro.ExtractAvroTreeBuilder
import org.kitesdk.morphline.avro.ReadAvroContainerBuilder
import org.kitesdk.morphline.base.Fields
import org.kitesdk.morphline.base.Notifications
import org.kitesdk.morphline.hadoop.parquet.avro.ReadAvroParquetFileBuilder
import org.kitesdk.morphline.stdio.ReadCSVBuilder
import org.kitesdk.morphline.stdlib.DropRecordBuilder
import org.kitesdk.morphline.stdlib.PipeBuilder
import org.kitesdk.morphline.stdlib.SampleBuilder
import org.slf4j.LoggerFactory
import org.schedoscope.DriverSettings
import org.schedoscope.Settings
import org.schedoscope.dsl.Avro
import org.schedoscope.dsl.ExternalAvro
import org.schedoscope.dsl.ExternalStorageFormat
import org.schedoscope.dsl.ExternalTextFile
import org.schedoscope.dsl.JDBC
import org.schedoscope.dsl.NullStorage
import org.schedoscope.dsl.Parquet
import org.schedoscope.dsl.Redis
import org.schedoscope.dsl.StorageFormat
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.transformations.MorphlineTransformation
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import Helper._
import morphlineutils.morphline.CommandConnector
import morphlineutils.morphline.MorphlineClasspathUtil
import morphlineutils.morphline.command.CSVWriterBuilder
import morphlineutils.morphline.command.JDBCWriterBuilder
import morphlineutils.morphline.command.REDISWriterBuilder
import morphlineutils.morphline.command.anonymization.AnonymizeAvroBuilder
import morphlineutils.morphline.command.anonymization.AnonymizeBuilder
import morphlineutils.morphline.command.sink.AvroWriterBuilder
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import org.schedoscope.dsl.ExaSolution
import morphlineutils.morphline.command.ExasolWriterBuilder

object Helper {
  implicit def AnyToConfigValue(x: Any) = ConfigValueFactory.fromAnyRef(x, "")
}

class MorphlineDriver(val ugi: UserGroupInformation, val hadoopConf: Configuration) extends Driver[MorphlineTransformation] {
  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")
  val context = new MorphlineContext.Builder().build()
  def transformationName: String = "morphline"
  val log = LoggerFactory.getLogger(classOf[MorphlineDriver])

  override def run(t: MorphlineTransformation): DriverRunHandle[MorphlineTransformation] = {
    val f = future {
      try {
        runMorphline(createMorphline(t), t)
      } catch {
        case e: Throwable => log.error("Error when creating morphline", e); DriverRunFailed[MorphlineTransformation](this, "could not create morphline ", e)
      }
    }

    new DriverRunHandle[MorphlineTransformation](this, new LocalDateTime(), t, f)
  }

  def createInput(format: StorageFormat, sourceTable: String, cols: Seq[String], finalCommand: Command): Command = {
    // stub to overcome checkNull()s when creating morphlines
    val root = new Command {
      override def getParent(): Command = null

      override def notify(notification: Record) {
        throw new UnsupportedOperationException("Root command should be invisible and must not be called")
      }

      override def process(record: Record): Boolean = {
        throw new UnsupportedOperationException("Root command should be invisible and must not be called")
      }
    }

    format match {
      case f: TextFile => {
        new ReadCSVBuilder().build(ConfigFactory.empty().
          withValue("separator", f.fieldTerminator).
          withValue("trim", true).
          withValue("ignoreFirstLine", false).
          withValue("charset", "UTF-8").
          withValue("commentPrefix", "#").
          withValue("columns", ConfigValueFactory.fromIterable(cols)),
          root, finalCommand, context)

      }

      case f: Parquet => {
        val iConfig = ConfigFactory.empty()
        new ReadAvroParquetFileBuilder().build(iConfig, root, finalCommand, context)
      }

      case f: Avro => {
        val iConfig = ConfigFactory.empty()
        new ReadAvroContainerBuilder().build(iConfig, root, finalCommand, context)
      }

      case _ => throw new UnsupportedOperationException("unsupported  input format for morphline")
    }
  }

  def createOutput(parent: Command, transformation: MorphlineTransformation): Command = {
    val view = transformation.view.get
    val storageFormat = view.storageFormat.asInstanceOf[ExternalStorageFormat]
    val child = new DropRecordBuilder().build(null, null, null, context);
    val commandConfig = ConfigFactory.empty()
    val fields = if (transformation.definition != "") view.fields.map(field => field.n)
    else view.fields.map(field => "/" + field.n)
    val command = storageFormat match {
      case f: ExaSolution => {
        val schema = view.fields.foldLeft(ConfigFactory.empty())((config, field) => config.withValue((if (transformation.definition == "") "/" else "") + field.n, field.t.erasure.getSimpleName()))
        val oConfig = commandConfig.withValue("connectionURL", f.jdbcUrl).
          withValue("keys", ConfigValueFactory.fromIterable(f.mergeKeys)).
          withValue("merge", f.merge).
          withValue("username", f.userName).
          withValue("schema", schema.root()).
          withValue("targetTable", view.n).
          withValue("password", f.password).
          withValue("fields", ConfigValueFactory.fromIterable(fields))
        new ExasolWriterBuilder().build(oConfig, parent, child, context)
      }

      case f: ExternalTextFile => {
        val oConfig = ConfigFactory.empty().withValue("filename", view.locationPath).
          withValue("separator", f.fieldTerminator).
          withValue("fields", ConfigValueFactory.fromIterable(fields))
        new CSVWriterBuilder().build(oConfig, parent, child, context)
      }

      case f: Redis => {
        log.info("creating RedisWriter")
        val keys = f.keys
        val oConfig = ConfigFactory.empty().withValue("server", f.host).
          withValue("port", f.port).withValue("password", f.password).
          withValue("keys", ConfigValueFactory.fromIterable(keys)).
          withValue("fields", ConfigValueFactory.fromIterable(fields))
        new REDISWriterBuilder().build(oConfig, parent, child, context)
      }

      case f: JDBC => {
        log.info("creating JDBCWriter")
        val schema = view.fields.foldLeft(ConfigFactory.empty())((config, field) => config.withValue(field.n, field.t.erasure.getSimpleName()))
        val oConfig = commandConfig.withValue("connectionURL", f.jdbcUrl).
          withValue("jdbcDriver", f.jdbcDriver).
          withValue("username", f.userName).
          withValue("schema", schema.root()).
          withValue("targetTable",
            view.n).withValue("password", f.password).
            withValue("fields", ConfigValueFactory.fromIterable(fields))
        new JDBCWriterBuilder().build(oConfig, parent, child, context)
      }

      case f: ExternalAvro => {
        log.info("creating AvroFile")
        val oConfig = commandConfig.withValue("filename", view.locationPath + "/000000")
        new AvroWriterBuilder().build(oConfig, parent, child, context)
      }

      case _: NullStorage => {
        child
      }
    }

    command
  }

  def createSampler(inputConnector: CommandConnector, sample: Double) = {
    log.info("creating sampler with rate " + sample)
    val sampleConfig = ConfigFactory.empty().withValue("probability", sample)
    val sampleConnector = new CommandConnector(false, "sampleconnector")
    sampleConnector.setParent(inputConnector.getParent)
    sampleConnector.setChild(inputConnector.getChild)
    val sampleCommand = new SampleBuilder().build(sampleConfig, inputConnector, sampleConnector, context)
    inputConnector.setChild(sampleCommand)
    sampleCommand
  }

  def createMorphline(transformation: MorphlineTransformation): Command = {
    val view = transformation.view.get
    val inputView = transformation.view.get.dependencies.head

    val inputConnector = new CommandConnector(false, "inputconnector")
    val outputConnector = new CommandConnector(false, "outputconnector")

    val inputCommand = createInput(inputView.storageFormat, inputView.n, inputView.fields.map(field => field.n), inputConnector)
    val finalCommand = createOutput(outputConnector, transformation)

    outputConnector.setChild(finalCommand)
    inputConnector.setParent(inputCommand)
    inputConnector.setChild(outputConnector)
    outputConnector.setParent(inputConnector)

    MorphlineClasspathUtil.setupJavaCompilerClasspath()
    // if the user did specify a morphline, plug it between reader and writer
    if (transformation.definition != "") {
      log.info("compiling morphline...")
      val morphlineConfig = ConfigFactory.parseString(transformation.definition)
      val morphline = new PipeBuilder().build(morphlineConfig, inputConnector, outputConnector, context)
      outputConnector.setParent(morphline)
      inputConnector.setChild(morphline)
      inputConnector.parent
    } else {
      inputView.storageFormat match {
        case _: TextFile => inputCommand

        case _ => { // if the user did not specify a morphline, at least extract all avro paths so they can be anonymized
          log.info("Empty morphline, inserting extractAvro")
          val extractAvroTreeCommand = new ExtractAvroTreeBuilder().build(ConfigFactory.empty(), inputConnector, outputConnector, context)
          inputConnector.setChild(extractAvroTreeCommand)
          outputConnector.setParent(extractAvroTreeCommand)
          extractAvroTreeCommand
        }
      }

      val sensitive = (view.fields.filter(_.isPrivacySensitive).map(_.n)) ++ transformation.anonymize
      if (!sensitive.isEmpty) {
        log.info("adding anonymizer")
        val anonConfig = ConfigFactory.empty().withValue("fields", ConfigValueFactory.fromIterable(sensitive))

        val output2Connector = new CommandConnector(false, "output2")
        output2Connector.setParent(outputConnector.getParent);
        output2Connector.setChild(outputConnector.getChild())

        val anonymizer = if (view.storageFormat.isInstanceOf[ExternalAvro])
          new AnonymizeAvroBuilder().build(anonConfig, outputConnector, output2Connector, context)
        else
          new AnonymizeBuilder().build(anonConfig, outputConnector, output2Connector, context)
        outputConnector.setChild(anonymizer)
      }

      if (transformation.sampling < 100)
        createSampler(inputConnector, transformation.sampling)
      else
        inputConnector.parent
    }
  }

  def runMorphline(command: Command, transformation: MorphlineTransformation): DriverRunState[MorphlineTransformation] = {
    Notifications.notifyBeginTransaction(command)
    Notifications.notifyStartSession(command)

    val driver = this
    ugi.doAs(new PrivilegedAction[DriverRunState[MorphlineTransformation]]() {
      override def run(): DriverRunState[MorphlineTransformation] = {
        try {
          val view = transformation.view.get
          view.dependencies.foreach { dep =>
            {
              val fs = FileSystem.get(new URI(dep.fullPath), hadoopConf)
              val test = fs.listStatus(new Path(dep.fullPath)).map { status =>
                val record: Record = new Record()
                if (!status.getPath().getName().startsWith("_")) {
                  log.info("processing morphline on " + status.getPath().toUri().toString())
                  val in: java.io.InputStream =
                    fs.open(status.getPath()).getWrappedStream().asInstanceOf[java.io.InputStream]
                  record.put(Fields.ATTACHMENT_BODY, in.asInstanceOf[java.io.InputStream]);
                  for (field <- view.partitionParameters)
                    record.put(field.n, field.v.get)
                  record.put("file_upload_url", status.getPath().toUri().toString())
                  try {
                    if (!command.process(record))
                      log.error("Morphline failed to process record: " + record);
                  } catch {
                    case e: Throwable => {
                      Notifications.notifyRollbackTransaction(command)
                      context.getExceptionHandler().handleException(e, null)

                    }
                  } finally {
                    in.close();
                  }
                }
              }
            }
          }
          Notifications.notifyCommitTransaction(command)
        } catch {
          case e: Throwable => {
            Notifications.notifyRollbackTransaction(command)
            context.getExceptionHandler().handleException(e, null)
            log.error("Morphline failed", e)
            return DriverRunFailed[MorphlineTransformation](driver, s"Morphline failed", e)
          }
        }
        Notifications.notifyShutdown(command);
        DriverRunSucceeded[MorphlineTransformation](driver, "Morphline succeeded")
      }
    })
  }
}

object MorphlineDriver {
  def apply(ds: DriverSettings) = new MorphlineDriver(Settings().userGroupInformation, Settings().hadoopConf)
}
