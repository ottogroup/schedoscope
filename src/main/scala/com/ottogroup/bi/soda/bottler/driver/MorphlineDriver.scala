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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedAction
import java.net.URI
import Helper._
import com.ottogroup.bi.soda.dsl.StorageFormat
import com.ottogroup.bi.soda.dsl.ExternalAvro
import com.ottogroup.bi.soda.dsl.Parquet
import com.ottogroup.bi.soda.dsl.ExternalTextFile
import com.ottogroup.bi.soda.dsl.ExaSol
import com.ottogroup.bi.soda.dsl.TextFile
import com.ottogroup.bi.soda.dsl.Redis
import com.ottogroup.bi.soda.dsl.JDBC
import com.ottogroup.bi.soda.dsl.NullStorage
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation
import com.ottogroup.bi.soda.dsl.ExternalStorageFormat
import com.ottogroup.bi.soda.dsl.transformations.MorphlineTransformation

object Helper {

	implicit def AnyToConfigValue(x: Any) = ConfigValueFactory.fromAnyRef(x, "command line")
}

class MorphlineDriver extends Driver[MorphlineTransformation] {
	implicit val executionContext = Settings().system.dispatchers.lookup("akka.actor.future-driver-dispatcher")
	val context = new MorphlineContext.Builder().build()
	override def run(t: MorphlineTransformation): DriverRunHandle[MorphlineTransformation] = {
		val f = future {
		  try {		  
			  runMorphline(createMorphline(t), t) }
		  catch {
		    case e:Throwable=> DriverRunFailed[MorphlineTransformation](this,"could not create morphline",e)
		  }
		}
		new DriverRunHandle[MorphlineTransformation](this, new LocalDateTime(), t, f)
	}


	override def runAndWait(t: MorphlineTransformation): DriverRunState[MorphlineTransformation] =
			Await.result(run(t).stateHandle.asInstanceOf[Future[DriverRunState[MorphlineTransformation]]], runTimeOut)
			// Members declared in com.ottogroup.bi.soda.bottler.driver.Driver
		
	  def transformationName: String = "morphline"
	  //factory method for morphline input (reader) commands
	  def createInput(format: StorageFormat, sourceTable: String, cols: Seq[String], finalCommand: Command): Command = {
	
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
	      case f: ExternalAvro => {
	        val iConfig = ConfigFactory.empty()
	        new ReadAvroContainerBuilder().build(iConfig, root, finalCommand, context)
	      }
	      case _ => throw new UnsupportedOperationException("unsupported  input format for morphline")
	
	    }
	  }
			// factory method for predefined output commands
	def createOutput(parent: Command,transformation:MorphlineTransformation): Command = {
						val view = transformation.view.get
						val storageFormat = view.storageFormat.asInstanceOf[ExternalStorageFormat]
						val child = new DropRecordBuilder().build(null, null, null, context);
						val commandConfig = ConfigFactory.empty()
						val fields = view.fields.map( field => field.n)
						val command =storageFormat match { 
						case f:ExaSol => {
							val oConfig = commandConfig.withValue("connectionURL", f.jdbcUrl).
									withValue("", "")
									new JDBCWriterBuilder().build(oConfig, parent, child, context)
						}
						case f:ExternalTextFile => {

							val oConfig = ConfigFactory.empty().withValue("filename", view.locationPath).
									withValue("separator", f.fieldTerminator).
									withValue("fields", ConfigValueFactory.fromIterable(fields))
									new CSVWriterBuilder().build(oConfig, parent, child, context)
						}
						case f:Redis => {

							val keys = f.keys
									val oConfig = ConfigFactory.empty().withValue("server", f.host).
									withValue("port",f.port).withValue("password", f.password).
									withValue("keys", ConfigValueFactory.fromIterable(keys)).
									withValue("fields", ConfigValueFactory.fromIterable(fields))
									new REDISWriterBuilder().build(oConfig, parent, child, context)
						}
						case f:JDBC => {
							val oConfig = commandConfig.withValue("connectionURL", f.jdbcUrl).
									withValue("jdbcDriver", f.jdbcDriver).
									withValue("username", f.userName).
									withValue("targetTable", view.tableName).withValue("password", f.password).
									withValue("fields", ConfigValueFactory.fromIterable(fields))
									new JDBCWriterBuilder().build(oConfig, parent, child, context)
						}
						case f:ExternalAvro => {
							val oConfig = commandConfig.withValue("filename",view.locationPath+"/000000")
									new AvroWriterBuilder().build(oConfig, parent, child, context)
						}
						case _:NullStorage => {
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

			def createMorphline(transformation: MorphlineTransformation):Command = {
			    val view = transformation.view.get				
				val outputConnector = new CommandConnector(false, "outputconnector");
				val finalCommand = createOutput(outputConnector, transformation)
				val inputView = transformation.view.get.dependencies
				outputConnector.setChild(finalCommand)
				val inputConnector = new CommandConnector(false, "inputconnector");
				val inputCommand = createInput( inputView.head.storageFormat, inputView.head.n, inputView.head.fields.map( field => field.n), inputConnector)
				inputConnector.setParent(inputCommand)
				inputConnector.setChild(outputConnector)
				outputConnector.setParent(inputConnector)
						MorphlineClasspathUtil.setupJavaCompilerClasspath()
						// if the user did specify a morphline, plug it between reader and writer
				if (transformation.definition!="") {
								val morphlineConfig = ConfigFactory.parseString(transformation.definition)
									val morphline = new PipeBuilder().build(morphlineConfig, inputConnector, outputConnector, context)
									outputConnector.setParent(morphline)
									inputConnector.setChild(morphline)
									morphline
				} else {inputView.head.storageFormat match  {
						  case _:TextFile => inputCommand
						
						  case _=> {	// if the user did not specify a morphline, at least extract all avro paths so they can be anonymized
							val extractAvroTreeCommand = new ExtractAvroTreeBuilder().build(ConfigFactory.empty(), inputConnector, outputConnector, context)
									inputConnector.setChild(extractAvroTreeCommand)
									outputConnector.setParent(extractAvroTreeCommand)
									extractAvroTreeCommand
						}
				}
				if (transformation.anonymize.size>0) {					
					val anonConfig = ConfigFactory.empty().withValue("fields", ConfigValueFactory.fromIterable(transformation.anonymize))
					val output2Connector = new CommandConnector(false, "output2")
					output2Connector.setParent(outputConnector.getParent);
					output2Connector.setChild(outputConnector.getChild())
					val anonymizer = if (view.storageFormat.isInstanceOf[ExternalAvro])
							new AnonymizeAvroBuilder().build(anonConfig, outputConnector, output2Connector, context)
						else
							new AnonymizeBuilder().build(anonConfig, outputConnector, output2Connector, context)
					outputConnector.setChild(anonymizer)
				}
				if (transformation.sampling<100)
					createSampler(inputConnector, transformation.sampling)
			   else
			     inputConnector.parent
				}
			}

			def runMorphline(command:Command,transformation:MorphlineTransformation):DriverRunState[MorphlineTransformation] = {
				Notifications.notifyBeginTransaction(command);
				Notifications.notifyStartSession(command);

			    val driver=this
				try {
					Settings().userGroupInformation.doAs(new PrivilegedAction[ DriverRunState[MorphlineTransformation]]() {
						override def run():DriverRunState[MorphlineTransformation] = {

								try {

											val view = transformation.view.get
											view.dependencies.foreach{dep => {
												val fs = FileSystem.get(new URI(dep.locationPath),Settings().hadoopConf)
												val test=fs.listStatus(new Path(dep.locationPath)).map { status =>
													val record: Record = new Record()
													if (!status.getPath().getName().startsWith("_")) {
													    
														val in :java.io.InputStream = 
														  fs.open(status.getPath()).getWrappedStream().asInstanceOf[java.io.InputStream]
													    record.put(Fields.ATTACHMENT_BODY, in.asInstanceOf[java.io.InputStream]);
														
														for (field <- view.partitionParameters)
															record.put(field.n,field.v)
														record.put("file_upload_url", status.getPath().toUri().toString())
														try {
														if (!command.process(record))
															println("Morphline failed to process record: " + record);
													    }catch {
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
											return   DriverRunFailed[MorphlineTransformation](driver, s"Morphline failed",e)
										  
										}
								}
								Notifications.notifyShutdown(command);
								DriverRunSucceeded[MorphlineTransformation](driver,"Morphline succeeded")
						}
					}
							)
				}
			}
}  
object MorphlineDriver {
	def apply(ds: DriverSettings) = new MorphlineDriver()
}
