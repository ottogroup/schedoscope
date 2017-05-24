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

import java.io.{File, IOException, InputStream}
import java.net.URI
import java.nio.file.Files
import java.security.PrivilegedAction

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.joda.time.LocalDateTime
import org.schedoscope.Schedoscope
import org.schedoscope.conf.DriverSettings
import org.schedoscope.dsl.transformations._
import org.schedoscope.test.resources.TestResources

import scala.concurrent.Future

/**
  * Driver for executing file system transformations
  */
class FilesystemDriver(val driverRunCompletionHandlerClassNames: List[String], val ugi: UserGroupInformation, val conf: Configuration) extends DriverOnBlockingApi[FilesystemTransformation] {

  def transformationName = "filesystem"

  /**
    * Construct a future-based driver run handle
    */
  override def run(t: FilesystemTransformation): DriverRunHandle[FilesystemTransformation] =
    new DriverRunHandle(this, new LocalDateTime(), t, Future {
      doRun(t)
    })

  /**
    * Actually perform the Filesystem operation and reture the run state as the result
    */
  def doRun(t: FilesystemTransformation): DriverRunState[FilesystemTransformation] =
    t match {

      case IfExists(path, op) => doAs(() => {
        if (fileSystem(path).exists(new Path(path)))
          doRun(op)
        else
          DriverRunSucceeded(this, s"Path ${path} does not yet exist.")
      })

      case IfNotExists(path, op) => doAs(() => {
        if (!fileSystem(path).exists(new Path(path)))
          doRun(op)
        else
          DriverRunSucceeded(this, s"Path ${path} already exists.")
      })

      case CopyFrom(from, view, recursive) => doAs(() => copy(from, view.fullPath, recursive))
      case StoreFrom(inputStream, view) => doAs(() => storeFromStream(inputStream, view.fullPath))
      case Copy(from, to, recursive) => doAs(() => copy(from, to, recursive))
      case Move(from, to) => doAs(() => move(from, to))
      case Delete(path, recursive) => doAs(() => delete(path, recursive))
      case MkDir(path) => doAs(() => mkdirs(path))
      case Touch(path) => doAs(() => touch(path))

      case _ => DriverRunFailed(this, "File system driver can only handle file transformations.", new UnsupportedOperationException("File system driver can only handle file transformations."))
    }

  /**
    * Encapsulates a filesystem transformation run as a PrivilegedAction to operate in kerberized clusters
    */
  def doAs(f: () => DriverRunState[FilesystemTransformation]): DriverRunState[FilesystemTransformation] = ugi.doAs(new PrivilegedAction[DriverRunState[FilesystemTransformation]]() {
    def run(): DriverRunState[FilesystemTransformation] = {
      f()
    }
  })

  /**
    * Writes all bytes from an given InputStream to a file in the view locationPath
    */
  def storeFromStream(inputStream: InputStream, to: String): DriverRunState[FilesystemTransformation] = try {
    val streamInFile = FilesystemDriver.inputStreamToFile(inputStream)

    val fromFS = fileSystem(streamInFile)
    val toFS = fileSystem(to)

    toFS.mkdirs(new Path(to))

    FileUtil.copy(fromFS, new Path(streamInFile), toFS, new Path(to), false, true, conf)

    DriverRunSucceeded(this, s"Storing from InputStream to ${to} succeeded")
  } catch {
    case i: IOException => DriverRunFailed(this, s"Caught IO exception while storing InputStream to ${to}", i)
    case t: Throwable => throw RetryableDriverException(s"Runtime exception caught while copying InputStream to ${to}", t)
  }


  /**
    *
    * (Recursively) copy all files from one location into another. Both locations need to be valid
    * URLs that a hadoop filesystem implementation can handle. Moreover, classpath URLs are supported.
    */
  def copy(from: String, to: String, recursive: Boolean): DriverRunState[FilesystemTransformation] = {

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
      // is the source a resource that resides in a JAR with classpath?
      val fromIncludingResources = if (from.startsWith("classpath://"))
        FilesystemDriver.classpathResourceToFile(from)
      else
        from

      val fromFS = fileSystem(fromIncludingResources)
      val toFS = fileSystem(to)
      inner(fromFS, toFS, listFiles(fromIncludingResources), new Path(to))

      DriverRunSucceeded(this, s"Copy from ${from} to ${to} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while copying ${from} to ${to}", i)
      case t: Throwable => throw RetryableDriverException(s"Runtime exception caught while copying ${from} to ${to}", t)
    }
  }

  /**
    * Delete files (recursively)
    */
  def delete(path: String, recursive: Boolean): DriverRunState[FilesystemTransformation] =
    try {
      val targetFS = fileSystem(path)
      val files = listFiles(path)
      files.foreach(status => targetFS.delete(status.getPath(), recursive))

      DriverRunSucceeded(this, s"Deletion of ${path} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while deleting ${path}", i)
      case t: Throwable => throw RetryableDriverException(s"Runtime exception while deleting ${path}", t)
    }

  /**
    * Create a file
    */
  def touch(path: String): DriverRunState[FilesystemTransformation] =
    try {
      val filesys = fileSystem(path)
      val toCreate = new Path(path)

      filesys.create(toCreate).close()

      DriverRunSucceeded(this, s"Touching of ${path} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while touching ${path}", i)
      case t: Throwable => throw RetryableDriverException(s"Runtime exception while touching ${path}", t)
    }

  /**
    * Create a directory path
    */
  def mkdirs(path: String): DriverRunState[FilesystemTransformation] =
    try {
      val filesys = fileSystem(path)

      filesys.mkdirs(new Path(path))

      DriverRunSucceeded(this, s"Touching of ${path} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while making dirs ${path}", i)
      case t: Throwable => throw RetryableDriverException(s"Runtime exception while making dirs ${path}", t)
    }

  /**
    * Moves files from one location to the other.
    */
  def move(from: String, to: String): DriverRunState[FilesystemTransformation] =
    try {
      val fromFS = fileSystem(from)
      val toFS = fileSystem(to)
      val files = listFiles(from)

      FileUtil.copy(fromFS, FileUtil.stat2Paths(files), toFS, new Path(to), true, true, conf)

      DriverRunSucceeded(this, s"Moving from ${from} to ${to} succeeded")
    } catch {
      case i: IOException => DriverRunFailed(this, s"Caught IO exception while  moving from ${from} to ${to}", i)
      case t: Throwable => throw RetryableDriverException(s"Runtime exception while moving from ${from} to ${to}", t)
    }

  def listFiles(path: String): Array[FileStatus] = {
    val files = fileSystem(path).globStatus(new Path(path))
    if (files != null)
      files
    else Array()
  }

  def localFilesystem: FileSystem = FilesystemDriver.localFilesystem(conf)

  def fileSystem(path: String) = FilesystemDriver.fileSystem(path, conf)

  override def deployAll(driverSettings: DriverSettings) = true
}

/**
  * Factory and helper methods for file system driver.
  */
object FilesystemDriver extends DriverCompanionObject[FilesystemTransformation] {

  def apply(ds: DriverSettings) =
    new FilesystemDriver(ds.driverRunCompletionHandlers, Schedoscope.settings.userGroupInformation, Schedoscope.settings.hadoopConf)

  def apply(driverSettings: DriverSettings, testResources: TestResources): Driver[FilesystemTransformation] =
    new FilesystemDriver(List("org.schedoscope.test.resources.TestDriverRunCompletionHandler"), testResources.ugi, new Configuration(true))

  private def uri(pathOrUri: String) =
    try {
      new URI(pathOrUri)
    } catch {
      case _: Throwable => new File(pathOrUri).toURI()
    }

  def localFilesystem(hadoopConfiguration: Configuration): FileSystem = synchronized(FileSystem.getLocal(hadoopConfiguration))

  def fileSystem(path: String, hadoopConfiguration: Configuration) = synchronized(FileSystem.get(uri(path), hadoopConfiguration))

  def defaultFileSystem(hadoopConfiguration: Configuration) = synchronized(FileSystem.get(hadoopConfiguration))

  def classpathResourceToFile(classpathResourceUrl: String) = {
    val remainingPath = classpathResourceUrl.replace("classpath://", "")
    val tempDir = Files.createTempDirectory("classpath").toFile.toString()
    val tempFile = new File(tempDir + File.separator + remainingPath)
    FileUtils.touch(tempFile)
    FileUtils.copyInputStreamToFile(this.getClass().getResourceAsStream("/" + remainingPath), tempFile)

    tempFile.toURI.toString
  }

  def inputStreamToFile(inputStream: InputStream) = {
    val remainingPath = "stream.out"
    val tempDir = Files.createTempDirectory("classpath").toFile.toString()
    val tempFile = new File(tempDir + File.separator + remainingPath)
    FileUtils.touch(tempFile)
    FileUtils.copyInputStreamToFile(inputStream, tempFile)

    tempFile.toURI.toString
  }
}
