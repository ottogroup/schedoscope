package org.schedoscope.dsl.transformations

import java.io.IOException
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Job, JobContext}
import org.apache.hadoop.tools.DistCpOptions.FileAttribute
import org.apache.hadoop.tools.mapred.CopyMapper
import org.apache.hadoop.tools.util.DistCpUtils
import org.apache.hadoop.tools.{DistCp, DistCpConstants, DistCpOptions}
import org.schedoscope.dsl.View
import org.schedoscope.scheduler.driver.{DriverRunState, MapreduceDriver}

import scala.collection.JavaConverters._


object DistCpTransformation {

  def copyToView(sourceView: View, targetView: View): DistCpTransformation = {
    val target = targetView.fullPath.split("/").dropRight(1).mkString("/")
    println(target)
    DistCpTransformation(targetView, List(sourceView.fullPath), target)
  }

  def copyToDirToView(sourcePath: String, targetView: View): DistCpTransformation = {
    val target = targetView.fullPath.split("/").drop(1).mkString("/")
    DistCpTransformation(targetView, List(sourcePath), target)
  }

  def copyToFileToView(sourceFile: String, targetView: View): DistCpTransformation = {
    DistCpTransformation(targetView, List(sourceFile), targetView.fullPath)
  }

}

case class DistCpTransformation(v: View,
                                var sources: List[String],
                                var target: String,
                                deleteViewPath: Boolean = false,
                                config: Configuration = new Configuration())
  extends MapreduceBaseTransformation {

  var directoriesToDelete = if (deleteViewPath) List(v.fullPath) else List()

  val internalJobSubmit = true

  val cleanupAfterJob: (Job, MapreduceDriver, DriverRunState[MapreduceBaseTransformation]) =>
    DriverRunState[MapreduceBaseTransformation] = (_, __, completionRunState) => completionRunState

  lazy val job: Job = new DistCp(config, distCpOptions).execute()

  def distCpOptions: DistCpOptions = if (configuration.nonEmpty) {
    DistCpConfiguration
      .fromConfig(configuration.toMap)
      .toDistCpOptions(sources.map(new Path(_)), new Path(target))
  } else {
    val s = sources.map(new Path(_)).asJava
    new DistCpOptions(s, new Path(target))
  }
}

object DistCpConfiguration {

  def create(): DistCpConfiguration = {
    new DistCpConfiguration()
  }

  implicit def toConfig(distCpConfiguration: DistCpConfiguration): Map[String, Any] = {
    distCpConfiguration.toConfiguration()
  }

  implicit def fromConfig(config: Map[String, Any]): DistCpConfiguration = {
    DistCpConfiguration.create().fromConfiguration(config)
  }
}

class DistCpConfiguration {

  var sourcePaths: List[Path] = _
  var targetPath: Path = _
  var atomicCommit = false
  var syncFolder = false
  var deleteMissing = false
  var ignoreFailures = false
  var overwrite = false
  var skipCRC = false
  var blocking = true
  var useDiff = false
  var useRdiff = false
  var numListstatusThreads: Int = 0
  var maxMaps: Int = DistCpConstants.DEFAULT_MAPS
  var mapBandwidth: Int = DistCpConstants.DEFAULT_BANDWIDTH_MB
  var sslConfigurationFile: String = _
  var copyStrategy = DistCpConstants.UNIFORMSIZE
  var preserveStatus: Set[FileAttribute] = Set()
  var preserveRawXattrs: Boolean = _
  var atomicWorkPath: Path = _
  var logPath: Path = _
  var sourceFileListing: Path = _
  var filtersFile: String = _
  var append = false
  var fromSnapshot: String = _
  var toSnapshot: String = _

  def fromConfiguration(configuration: Map[String, Any]): DistCpConfiguration = {
    sourcePaths = configuration("sourcePaths").asInstanceOf[List[Path]]
    targetPath = configuration("targetPath").asInstanceOf[Path]
    atomicCommit = configuration("atomicCommit").asInstanceOf[Boolean]
    syncFolder = configuration("syncFolder").asInstanceOf[Boolean]
    deleteMissing = configuration("deleteMissing").asInstanceOf[Boolean]
    ignoreFailures = configuration("ignoreFailures").asInstanceOf[Boolean]
    overwrite = configuration("overwrite").asInstanceOf[Boolean]
    append = configuration("append").asInstanceOf[Boolean]
    useDiff = configuration("useDiff").asInstanceOf[Boolean]
    useRdiff = configuration("useRdif").asInstanceOf[Boolean]
    fromSnapshot = configuration("fromSnapshot").asInstanceOf[String]
    toSnapshot = configuration("toSnapshot").asInstanceOf[String]
    skipCRC = configuration("skipCRC").asInstanceOf[Boolean]
    blocking = configuration("blocking").asInstanceOf[Boolean]
    numListstatusThreads = configuration("numListstatusThreads").asInstanceOf[Int]
    maxMaps = configuration("maxMaps").asInstanceOf[Int]
    mapBandwidth = configuration("mapBandwidth").asInstanceOf[Int]
    sslConfigurationFile = configuration("sslConfigurationFile'").asInstanceOf[String]
    copyStrategy = configuration("copyStrategy").asInstanceOf[String]
    preserveStatus = configuration("preserveStatus").asInstanceOf[Set[FileAttribute]]
    preserveRawXattrs = configuration("preserveRawXattrs").asInstanceOf[Boolean]
    atomicWorkPath = configuration("atomicWorkPath").asInstanceOf[Path]
    logPath = configuration("logPath").asInstanceOf[Path]
    sourceFileListing = configuration("sourceFileListing").asInstanceOf[Path]
    sourcePaths = configuration("sourcePaths").asInstanceOf[List[Path]]
    targetPath = configuration("targetPath").asInstanceOf[Path]
    filtersFile = configuration("filtersFile").asInstanceOf[String]
    this
  }

  def toConfiguration(): Map[String, Any] = Map(
    "sourcePaths" -> sourcePaths,
    "targetPath" -> targetPath,
    "atomicCommit" -> atomicCommit,
    "syncFolder" -> syncFolder,
    "deleteMissing" -> deleteMissing,
    "ignoreFailures" -> ignoreFailures,
    "overwrite" -> overwrite,
    "append" -> append,
    "useDiff" -> useDiff,
    "useRdif" -> useRdiff,
    "fromSnapshot" -> fromSnapshot,
    "toSnapshot" -> toSnapshot,
    "skipCRC" -> skipCRC,
    "blocking" -> blocking,
    "numListstatusThreads" -> numListstatusThreads,
    "maxMaps" -> maxMaps,
    "mapBandwidth" -> mapBandwidth,
    "sslConfigurationFile'" -> sslConfigurationFile,
    "copyStrategy" -> copyStrategy,
    "preserveStatus" -> preserveStatus,
    "preserveRawXattrs" -> preserveRawXattrs,
    "atomicWorkPath" -> atomicWorkPath,
    "logPath" -> logPath,
    "sourceFileListing" -> sourceFileListing,
    "sourcePaths" -> sourcePaths,
    "targetPath" -> targetPath,
    "filtersFile" -> filtersFile
  )

  def toDistCpOptions(source: List[Path], target: Path): DistCpOptions = {
    //create options
    val s = if (sourcePaths == null) source else sourcePaths
    val t = if (targetPath == null) target else targetPath
    val options = if (sourceFileListing != null) {
      new DistCpOptions(sourceFileListing, t)
    } else {
      new DistCpOptions(s.asJava, t)
    }

    //fill options
    options.setAtomicCommit(atomicCommit)
    options.setSyncFolder(syncFolder)
    options.setDeleteMissing(deleteMissing)
    options.setIgnoreFailures(ignoreFailures)
    options.setOverwrite(overwrite)
    options.setAppend(append)
    if (useDiff) options.shouldUseDiff()
    if (useRdiff) options.shouldUseRdiff()
    if (fromSnapshot != null && toSnapshot != null) {
      if (useDiff) {
        options.setUseDiff(fromSnapshot, toSnapshot)
      } else if (useRdiff) {
        options.setUseRdiff(fromSnapshot, toSnapshot)
      } else {
        throw new IllegalArgumentException("Set useDiff or useRdiff")
      }
    }
    options.setSkipCRC(skipCRC)
    options.setBlocking(blocking)
    options.setNumListstatusThreads(numListstatusThreads)
    options.setMaxMaps(maxMaps)
    if (sslConfigurationFile != null) options.setSslConfigurationFile(sslConfigurationFile)
    options.setCopyStrategy(copyStrategy)
    preserveStatus.foreach(options.preserve)
    if (preserveRawXattrs) options.preserveRawXattrs()
    if (atomicWorkPath != null) options.setAtomicWorkPath(atomicWorkPath)
    if (logPath != null) options.setLogPath(logPath)
    if (filtersFile != null) options.setFiltersFile(filtersFile)


    options
  }

}
