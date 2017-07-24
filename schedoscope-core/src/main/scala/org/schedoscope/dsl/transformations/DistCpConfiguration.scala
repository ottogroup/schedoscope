package org.schedoscope.dsl.transformations

import org.apache.hadoop.fs.Path
import org.apache.hadoop.tools.DistCpOptions.FileAttribute
import org.apache.hadoop.tools.{DistCpConstants, DistCpOptions}
import scala.collection.JavaConverters._

class DistCpConfiguration {

  /**
    * List of source paths. Setting this will overwrite the sources parameter of [[DistCpTransformation]].
    */
  var sourcePaths: List[Path] = _

  /**
    * Target path. Setting this will overwrite the target parameter of [[DistCpTransformation]].
    */
  var targetPath: Path = _

  /**
    * Enable atomic commit. Data will either be available at final target in a complete and consistent form, or not at all.
    */
  var atomicCommit = false

  /**
    * Set if source and target folder contents be sync'ed up.
    */
  var update = false

  /**
    * Delete the files existing in the dst but not in src.
    */
  var deleteMissing = false

  /**
    * Set if failures during copy be ignored.
    */
  var ignoreFailures = false

  /**
    * Overwrite destination.
    */
  var overwrite = false

  /**
    * Whether to skip CRC checks between source and target paths.
    */
  var skipCRC = false

  /**
    * Set if Disctp should run blocking or non-blocking
    */
  var blocking = true

  /**
    * Use snapshot diff report between given two snapshots to identify the difference between source and target,
    * and apply the diff to the target to make it in sync with source.
    *
    * This option is valid only with [[update]] option and the following conditions should be satisfied.
    * <p><ul>
    * <li> Both the source and the target FileSystem must be DistributedFileSystem.
    * <li> Two snapshots [[fromSnapshot]] and [[toSnapshot]] have been created on the source FS,
    *      and [[fromSnapshot]] is older than [[toSnapshot]].
    * <li> The target has the same snapshot [[fromSnapshot]]. No changes have been made
    *      on the target since [[fromSnapshot]] was created, thus [[fromSnapshot]] has the same content
    *      as the current state of the target.
    *      All the files/directories in the target are the same with source’s [[fromSnapshot]].
    * </ul><p>
    */
  var useDiff = false

  /**
    * Use snapshot diff report between given two snapshots to identify what has been changed on the target
    * since the snapshot [[fromSnapshot]] was created on the target, and apply the diff reversely to the target,
    * and copy modified files from the source’s [[fromSnapshot]], to make the target the same as [[fromSnapshot]].
    *
    * This option is valid only with [[update]] option and the following conditions should be satisfied.
    * <p><ul>
    * <li> Both the source and the target FileSystem must be DistributedFileSystem. The source and the target can be
    *      two different clusters/paths, or they can be exactly the same cluster/path. In the latter case,
    *      modified files are copied from target’s <oldSnapshot> to target’s current state).
    * <li> Two snapshots <newSnapshot> and <oldSnapshot> have been created on the target FS,
    *      and <oldSnapshot> is older than <newSnapshot>. No change has been made on target
    *      since <newSnapshot> was created on the target.
    * <li> The source has the same snapshot <oldSnapshot>, which has the same content as the <oldSnapshot> on the target.
    *      All the files/directories in the target’s <oldSnapshot> are the same with source’s <oldSnapshot>.
    * </ul><p>
    */
  var useRdiff = false

  /**
    * Set the number of threads to use for listStatus. We allow max 40
    * threads. Setting numThreads to zero signify we should use the value
    * from conf properties.
    */
  var numListstatusThreads: Int = 0

  /**
    * Set the max number of maps to use for copy
    */
  var maxMaps: Int = DistCpConstants.DEFAULT_MAPS

  /**
    *  	Specify bandwidth per map, in MB/second.
    */
  var mapBandwidth: Int = DistCpConstants.DEFAULT_BANDWIDTH_MB

  /**
    * Set the SSL configuration file path to use with hftps:// (local path)
    */
  var sslConfigurationFile: String = _

  /**
    * Set the copy strategy to use. Should map to a strategy implementation
    * in distp-default.xml
    */
  var copyStrategy = DistCpConstants.UNIFORMSIZE

  /**
    *  A set of file attributes that need to be preserved.
    */
  var preserveStatus: Set[FileAttribute] = Set()

  /**
    * Indicate that raw.* xattrs should be preserved
    */
  var preserveRawXattrs: Boolean = _

  /**
    * Set the tmp folder for atomic commit.
    */
  var atomicWorkPath: Path = _

  /**
    * Set the log path where distcp output logs are stored
    * Uses JobStagingDir/_logs by default.
    */
  var logPath: Path = _

  /**
    * File containing list of source paths. This will overwrite [[sourcePaths]].
    */
  var sourceFileListing: Path = _

  /**
    * The path to a list of patterns to exclude from copy.
    */
  var filtersFile: String = _

  /**
    * Set if we want to append new data to target files. This is valid only with
    * [[update]] option and CRC is not skipped.
    */
  var append = false

  /**
    * Set the old snapshot folder for [[useDiff]]/[[useRdiff]]
    */
  var fromSnapshot: String = _

  /**
    * Set the new snapshot folder for [[useDiff]]/[[useRdiff]]
    */
  var toSnapshot: String = _

  def fromConfiguration(configuration: Map[String, Any]): DistCpConfiguration = {
    sourcePaths = configuration("sourcePaths").asInstanceOf[List[Path]]
    targetPath = configuration("targetPath").asInstanceOf[Path]
    atomicCommit = configuration("atomicCommit").asInstanceOf[Boolean]
    update = configuration("syncFolder").asInstanceOf[Boolean]
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
    "syncFolder" -> update,
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
    options.setSyncFolder(update)
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

object DistCpConfiguration {

  def apply(): DistCpConfiguration = {
    new DistCpConfiguration()
  }

  implicit def toConfig(distCpConfiguration: DistCpConfiguration): Map[String, Any] = {
    distCpConfiguration.toConfiguration()
  }

  implicit def fromConfig(config: Map[String, Any]): DistCpConfiguration = {
    DistCpConfiguration().fromConfiguration(config)
  }
}