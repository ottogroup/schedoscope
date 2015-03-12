package com.ottogroup.bi.soda.bottler.driver

import com.ottogroup.bi.soda.dsl.Transformation
import akka.actor.Actor
import com.ottogroup.bi.soda.dsl.transformations.filesystem._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.Path
import com.ottogroup.bi.soda.dsl.transformations.filesystem.IfNotExists
import akka.actor.Props
import akka.actor.Actor
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import scala.concurrent._
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.PathFilter
import scala.util.matching.Regex
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedAction
import org.apache.hadoop.fs.FileStatus
import akka.actor.ActorRef
import akka.event.Logging
import java.io.File
import com.typesafe.config.Config
import com.ottogroup.bi.soda.bottler.api.Settings
import com.ottogroup.bi.soda.bottler.api.SettingsImpl
import com.ottogroup.bi.soda.bottler.api.DriverSettings
import org.apache.commons.io.FileUtils
import java.nio.file.Files
import scala.collection.mutable.HashMap
import org.joda.time.LocalDateTime
import scala.concurrent.duration.FiniteDuration
import scala.concurrent._
import scala.concurrent.duration.Duration
import java.io.IOException
import FileSystemDriver._

class FileSystemDriver(val ugi: UserGroupInformation, val conf: Configuration) extends Driver[FilesystemTransformation] {    

  override def runTimeOut: Duration = Settings().fileActionTimeout

  def run(t: FilesystemTransformation): DriverRunHandle[FilesystemTransformation] =
    new DriverRunHandle(this, new LocalDateTime(), t, null, future {
      doRun(t)
    }(ExecutionContext.global))

  def doRun(t: FilesystemTransformation): DriverRunState[FilesystemTransformation] =
    t match {

      case IfExists(path, op) => doAs(() => {
        if (fileSystem(path, conf).exists(new Path(path)))
          doRun(op)
        else
          DriverRunSucceeded(this, s"Path ${path} does not yet exist.")
      })

      case IfNotExists(path, op) => doAs(() => {
        if (!fileSystem(path, conf).exists(new Path(path)))
          doRun(op)
        else
          DriverRunSucceeded(this, s"Path ${path} already exists.")
      })

      case CopyFrom(from, view, recursive) => doAs(() => copy(from, view.fullPath, recursive))
      case Copy(from, to, recursive) => doAs(() => copy(from, to, recursive))
      case Move(from, to) => doAs(() => move(from, to))
      case Delete(path, recursive) => doAs(() => delete(path, recursive))
      case Touch(path) => doAs(() => touch(path))

      case _ => throw DriverException("FileSystemDriver can only run file transformations.")
    }

  def doAs(f: () => DriverRunState[FilesystemTransformation]): DriverRunState[FilesystemTransformation] = ugi.doAs(new PrivilegedAction[DriverRunState[FilesystemTransformation]]() {
    def run(): DriverRunState[FilesystemTransformation] = {
      f()
    }
  })

  def copy(from: String, to: String, recursive: Boolean): DriverRunState[FilesystemTransformation] = {
    def classpathResourceToFile(classpathResourceUrl: String) = {
      val remainingPath = classpathResourceUrl.replace("classpath://", "")
      val tempDir = Files.createTempDirectory("classpath").toFile.toString()
      val tempFile = new File(tempDir + File.separator + remainingPath)
      FileUtils.touch(tempFile)
      FileUtils.copyInputStreamToFile(this.getClass().getResourceAsStream("/" + remainingPath), tempFile)

      "file:" + tempFile.toString()
    }

    def inner(fromFS: FileSystem, toFS: FileSystem, files: Seq[FileStatus], to: Path): Unit = {
      toFS.mkdirs(to)
      if (recursive) {
        files.filter(p => (p.isDirectory() && !p.getPath().getName().startsWith("."))).
          foreach(path => {
            inner(fromFS, toFS, fromFS.globStatus(new Path(path.getPath(), "*")), new Path(to, path.getPath().getName()))
          })
      }
      files.filter(p => !p.isDirectory()).map(status => status.getPath()).foreach { p =>
        FileUtil.copy(fromFS, p, toFS, to, false, true, conf)
      }
    }

    try {
      val fromIncludingResources = if (from.startsWith("classpath://"))
        classpathResourceToFile(from)
      else
        from

      val fromFS = fileSystem(fromIncludingResources, conf)
      val toFS = fileSystem(to, conf)
      inner(fromFS, toFS, listFiles(fromIncludingResources), new Path(to))

      DriverRunSucceeded(this, s"Copy from ${from} to ${to} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while copying ${from} to ${to}", i)
      case t: Throwable => throw DriverException(s"Runtime exception caught while copying ${from} to ${to}", t)
    }
  }

  def delete(from: String, recursive: Boolean): DriverRunState[FilesystemTransformation] =
    try {
      val fromFS = fileSystem(from, conf)
      val files = listFiles(from)
      files.foreach(status => fromFS.delete(status.getPath(), recursive))

      DriverRunSucceeded(this, s"Deletion of ${from} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while copying ${from}", i)
      case t: Throwable => throw DriverException(s"Runtime exception while copying ${from}", t)
    }

  def touch(path: String): DriverRunState[FilesystemTransformation] =
    try {
      val filesys = fileSystem(path, conf)

      filesys.create(new Path(path))

      DriverRunSucceeded(this, s"Touching of ${path} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while touching ${path}", i)
      case t: Throwable => throw DriverException(s"Runtime exception while touching ${path}", t)
    }

  def mkdirs(path: String): DriverRunState[FilesystemTransformation] =
    try {
      val filesys = fileSystem(path, conf)

      filesys.mkdirs(new Path(path))

      DriverRunSucceeded(this, s"Touching of ${path} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while making dirs ${path}", i)
      case t: Throwable => throw DriverException(s"Runtime exception while making dirs ${path}", t)
    }

  def move(from: String, to: String): DriverRunState[FilesystemTransformation] =
    try {
      val fromFS = fileSystem(from, conf)
      val toFS = fileSystem(to, conf)
      val files = listFiles(from)

      FileUtil.copy(fromFS, FileUtil.stat2Paths(files), toFS, new Path(to), true, true, conf)

      DriverRunSucceeded(this, s"Moving from ${from} to ${to} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while  moving from ${from} to ${to}", i)
      case t: Throwable => throw DriverException(s"Runtime exception while moving from ${from} to ${to}", t)
    }

  def fileChecksums(paths: List[String], recursive: Boolean): List[String] = {
    paths.flatMap(p => {
      val fs = fileSystem(p, conf)
      val path = new Path(p)
      if (fs.isFile(path)) {
        val cs = ChecksumCache.lookup(p).getOrElse(fs.getFileChecksum(path))
        if (cs != null) {
          println("Computing checksum for " + p)
          List(ChecksumCache.cache(p, cs.toString))
        } else
          List()
      } else if (recursive) {
        fileChecksums(listFiles(p + "/*").map(f => f.getPath.toString()).toList, recursive)
      } else
        List()
    }).toList
  }

  def listFiles(path: String): Array[FileStatus] = {
    val files = fileSystem(path, conf).globStatus(new Path(path))
    if (files != null)
      files
    else Array()
  }

  def localFilesystem: FileSystem = FileSystem.getLocal(conf)

  def filesystem = FileSystem.get(conf)

  override def deployAll(driverSettings: DriverSettings) = true
}

object FileSystemDriver extends NamedDriver {
  private def uri(pathOrUri: String) =
    try {
      new URI(pathOrUri)
    } catch {
      case _: Throwable => new File(pathOrUri).toURI()
    }

  def fileSystem(path: String, conf: Configuration) = FileSystem.get(uri(path), conf)

  def apply(ds: DriverSettings) = {
    new FileSystemDriver(Settings().userGroupInformation, Settings().hadoopConf)
  }
}

object ChecksumCache {
  val checksums = HashMap[String, String]()

  def lookup(file: String): Option[String] = {
    checksums.get(file)
  }

  def cache(file: String, checksum: String): String = {
    checksums.put(file, checksum)
    checksum
  }

  def clear() {
    checksums.clear()
  }
}