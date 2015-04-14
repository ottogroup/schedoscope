package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Random
import org.joda.time.LocalDateTime
import com.ottogroup.bi.soda.DriverSettings
import com.ottogroup.bi.soda.dsl.Transformation
import net.lingala.zip4j.core.ZipFile
import com.ottogroup.bi.soda.Settings
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import com.ottogroup.bi.eci.transformations.morphline._
import scala.concurrent.future
import org.kitesdk.morphline.avro.ExtractAvroTreeBuilder
import org.kitesdk.morphline.hadoop.parquet.avro.ReadAvroParquetFileBuilder
import com.ottogroup.bi.eci.transformations.morphline.command.JDBCWriterBuilder
import com.typesafe.config.ConfigFactory
import org.kitesdk.morphline.stdlib.SampleBuilder
import org.kitesdk.morphline.api.MorphlineContext
import org.kitesdk.morphline.avro.ReadAvroContainerBuilder
import com.ottogroup.bi.eci.transformations.morphline.command.anonymization.AnonymizeAvroBuilder
import org.kitesdk.morphline.stdio.ReadCSVBuilder
import org.kitesdk.morphline.stdlib.DropRecordBuilder
import com.ottogroup.bi.eci.transformations.morphline.command.sink.AvroWriterBuilder
import com.ottogroup.bi.eci.transformations.morphline.command.anonymization.AnonymizeBuilder
import com.ottogroup.bi.eci.transformations.morphline.command.CSVWriterBuilder
import com.ottogroup.bi.eci.transformations.morphline.command.REDISWriterBuilder
import org.kitesdk.morphline.stdlib.PipeBuilder
import org.kitesdk.morphline.api.Command
import com.typesafe.config.Config
import org.kitesdk.morphline.base.Notifications
import org.kitesdk.morphline.api.Record
import org.kitesdk.morphline.api.MorphlineContext
import org.kitesdk.morphline.api.Record
import org.kitesdk.morphline.base.Compiler
import org.kitesdk.morphline.base.Fields
import org.kitesdk.morphline.base.Notifications
import com.typesafe.config.ConfigValueFactory
import scala.Option.option2Iterable
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedAction
import Helper._
import com.ottogroup.bi.soda.dsl.StorageFormat
import com.ottogroup.bi.soda.dsl.Avro
import com.ottogroup.bi.soda.dsl.Parquet
import com.ottogroup.bi.soda.dsl.TextFile

object Helper {

  implicit def AnyToConfigValue(x: Any) = ConfigValueFactory.fromAnyRef(x, "command line")
}

class MorphlineDriver extends Driver[MorphlineTransformation] {
  implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")
  val context = new MorphlineContext.Builder().build()
  override def run(t: MorphlineTransformation): DriverRunHandle[MorphlineTransformation] = {
    val f = future {

    }
    new DriverRunHandle[MorphlineTransformation](this, new LocalDateTime(), t, f)
  }

  override def runAndWait(t: MorphlineTransformation): DriverRunState[MorphlineTransformation] =
    Await.result(run(t).stateHandle.asInstanceOf[Future[DriverRunState[MorphlineTransformation]]], runTimeOut)
  // Members declared in com.ottogroup.bi.soda.bottler.driver.Driver
  def transformationName: String = "morphline"
  //factory method for morphline input (reader) commands
  def createInput(config: Config, format: StorageFormat, sourceTable: String, cols: Seq[String], finalCommand: Command): Command = {

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

    }
  }
  // factory method for predefined output commands
  def createOutput(config: Config, parent: Command): Command = {
    val child = new DropRecordBuilder().build(null, null, null, context);
    val commandConfig = ConfigFactory.empty().withValue("delta", config.getBoolean("delta"))
    val fields = config.getString("cols").split(',').toList
    val command = config.getString("sink") match {
      case "exasol" => {
        val oConfig = commandConfig.withValue("connectionURL", "jdbc:exasol://exasolhost").
          withValue("", "")
        new JDBCWriterBuilder().build(oConfig, parent, child, context)
      }
      case "csv" => {

        val oConfig = ConfigFactory.empty().withValue("filename", config.getString("output")).
          withValue("separator", config.getString("separator")).
          withValue("fields", ConfigValueFactory.fromIterable(fields))
        new CSVWriterBuilder().build(oConfig, parent, child, context)
      }
      case "redis" => {

        val keys = config.getString("keys").split(',').toList
        val oConfig = ConfigFactory.empty().withValue("server", config.getString("host")).
          withValue("port", config.getInt("port")).withValue("password", config.getString("password")).
          withValue("keys", ConfigValueFactory.fromIterable(keys)).
          withValue("fields", ConfigValueFactory.fromIterable(fields))
        new REDISWriterBuilder().build(oConfig, parent, child, context)
      }
      case "jdbc" => {
        val oConfig = commandConfig.withValue("connectionURL", config.getString("jdbcurl")).
          withValue("jdbcDriver", config.getString("jdbcdriver")).
          withValue("username", config.getString("username")).
          withValue("targetTable", config.getString("table")).withValue("password", config.getString("password")).
          withValue("fields", ConfigValueFactory.fromIterable(fields))
        new JDBCWriterBuilder().build(oConfig, parent, child, context)
      }
      case "avro" => {
        val oConfig = commandConfig.withValue("filename", config.getString("output"))
        new AvroWriterBuilder().build(oConfig, parent, child, context)
      }
      case "null" => {
        child
      }
    }
    command
  }
  def createSampler(inputConnector: CommandConnector, sample: Double) = {
    val sampleConfig = ConfigFactory.empty().withValue("probability", sample)
    val sampleConnector = new CommandConnector(false, "sampleconnector")
    sampleConnector.setParent(inputConnector.getParent)
    sampleConnector.setChild(inputConnector.getChild)
    val sampleCommand = new SampleBuilder().build(sampleConfig, inputConnector, sampleConnector, context)
    inputConnector.setChild(sampleCommand)
    sampleCommand
  }

  def createMorphline(transformation: MorphlineTransformation) = {
    val config = ConfigFactory.parseString(transformation.definition)
    val outputConnector = new CommandConnector(false, "outputconnector");
    val finalCommand = createOutput(config, outputConnector)
    outputConnector.setChild(finalCommand)
    val inputConnector = new CommandConnector(false, "inputconnector");
    val inputCommand = createInput(config, transformation.view.get.storageFormat, transformation.view.get.dependencies.head.n, Seq[String](), inputConnector)
    inputConnector.setParent(inputCommand)
    MorphlineClasspathUtil.setupJavaCompilerClasspath()
    // if the user did specify a morphline, plug it between reader and writer
    val parent = if (config.hasPath("morphline")) {
      val morphlineId = config.getString("morphline")
      val morphlineConfig = new Compiler().find(morphlineId, config, "error")
      val morphline = new PipeBuilder().build(morphlineConfig, inputConnector, outputConnector, context)
      outputConnector.setParent(morphline)
      inputConnector.setChild(morphline)
      morphline
    } else {
      // if the user did not specify a morphline, at least extract all avro paths so they can be anonymized
      val extractAvroTreeCommand = new ExtractAvroTreeBuilder().build(ConfigFactory.empty(), inputConnector, outputConnector, context)
      inputConnector.setChild(extractAvroTreeCommand)
      outputConnector.setParent(extractAvroTreeCommand)
      extractAvroTreeCommand
    }
    if (config.hasPath("anon")) {
      val anFields = config.getString("anon").split(",").toList
      val anonConfig = ConfigFactory.empty().withValue("fields", ConfigValueFactory.fromIterable(anFields))
      val output2Connector = new CommandConnector(false, "output2")
      output2Connector.setParent(outputConnector.getParent);
      output2Connector.setChild(outputConnector.getChild())

      val anonymizer = if (config.getString("sink").equals("avro"))
        new AnonymizeAvroBuilder().build(anonConfig, outputConnector, output2Connector, context)
      else
        new AnonymizeBuilder().build(anonConfig, outputConnector, output2Connector, context)
      outputConnector.setChild(anonymizer)
    }
    if (config.hasPath("sample"))
      createSampler(inputConnector, config.getDouble("sample"))
  }

}

object MorphlineDriver {
  def apply(ds: DriverSettings) = new MorphlineDriver()
}
