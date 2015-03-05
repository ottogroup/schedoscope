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
import FileSystemDriver._
import org.apache.commons.io.FileUtils
import java.nio.file.Files
import scala.collection.mutable.HashMap

class FileSystemDriver(val ugi: UserGroupInformation, conf: Configuration) extends Driver {

  def doAs(f: () => Boolean): Boolean = ugi.doAs(new PrivilegedAction[Boolean]() {
    def run(): Boolean = {
      f()
    }
  })  

  def runAndWait(t: Transformation): Boolean =
    t match {
      case IfExists(path, op) => doAs(() => {
        if (fileSystem(path, conf).exists(new Path(path)))
          runAndWait(op) else true
      })

      case IfNotExists(path, op) => doAs(() => {
        if (!fileSystem(path, conf).exists(new Path(path)))
          runAndWait(op) else true
      })

      case CopyFrom(from, view, recursive) => doAs(() => copy(from, view.fullPath, recursive))
      case Copy(from, to, recursive) => doAs(() => copy(from, to, recursive))
      case Move(from, to) => doAs(() => move(from, to))
      case Delete(path, recursive) => doAs(() => delete(path, recursive))
      case Touch(path) => doAs(() => touch(path))
    }

  def copy(from: String, to: String, recursive: Boolean) = {
    def classpathResourceToFile(classpathResourceUrl: String) = {
      val remainingPath = classpathResourceUrl.replace("classpath://", "")
      val tempDir = Files.createTempDirectory("classpath").toFile.toString()
      val tempFile = new File(tempDir + File.separator + remainingPath)
      FileUtils.touch(tempFile)
      FileUtils.copyInputStreamToFile(this.getClass().getResourceAsStream("/" + remainingPath), tempFile)

      tempFile.toString()
    }

    val fromIncludingResources = if (from.startsWith("classpath://"))
      classpathResourceToFile(from)
    else
      from

    val fromFS = fileSystem(fromIncludingResources, conf)
    val toFS = fileSystem(to, conf)
    val files = listFiles(fromIncludingResources)

    def inner(files: Seq[FileStatus], to: Path): Unit = {
      toFS.mkdirs(to)
      if (recursive) {
        files.filter(p => (p.isDirectory() && !p.getPath().getName().startsWith("."))).
          foreach(path => {
            inner(fromFS.globStatus(new Path(path.getPath(), "*")), new Path(to, path.getPath().getName()))
          })
      }
      files.filter(p => !p.isDirectory()).map(status => status.getPath()).foreach { p =>
        FileUtil.copy(fromFS, p, toFS, to, false, true, conf)
      }
    }

    try {
      inner(files, new Path(to))
    } catch {
      case e: Throwable => false
    }
    true

  }

  def delete(from: String, recursive: Boolean) = {
    val fromFS = fileSystem(from, conf)
    val files = listFiles(from)
    try {
      files.foreach(status => fromFS.delete(status.getPath(), recursive))
    } catch {
      case e: Throwable => false
    }
    true
  }

  def touch(path: String) = {
    val filesys = fileSystem(path, conf)
    try {
      filesys.create(new Path(path))
    } catch {
      case e: Throwable => false
    }
    true
  }

  def mkdirs(path: String) = {
    val filesys = fileSystem(path, conf)
    try {
      filesys.mkdirs(new Path(path))
    } catch {
      case e: Throwable => false
    }
    true
  }

  def move(from: String, to: String) = {
    val fromFS = fileSystem(from, conf)
    val toFS = fileSystem(to, conf)
    val files = listFiles(from)
    try {
      FileUtil.copy(fromFS, FileUtil.stat2Paths(files), toFS, new Path(to), true, true, conf)
    } catch {
      case e: Throwable => false
    }
    true
  }
  
  def fileChecksums(paths: List[String], recursive: Boolean) : List[String] = {
    paths.flatMap( p => {
      println("Computing checksum for " + p)
      val fs = fileSystem(p, conf)
      val path = new Path(p)
      if (fs.isFile(path) ) {
        val cs = ChecksumCache.lookup(p).getOrElse(fs.getFileChecksum(path))        
        if (cs != null) 
          List(ChecksumCache.cache(p, cs.toString))
        else
          List()
      }
      else if (recursive) {
        fileChecksums(listFiles(p + "/*").map( f => f.getPath.toString()).toList, recursive)
      }
      else
        List()
    }).toList 
  }
  

  def listFiles(path: String): Array[FileStatus] = {
    fileSystem(path, conf).globStatus(new Path(path))
  }

  def localFilesystem: FileSystem = FileSystem.getLocal(conf)

  def filesystem = FileSystem.get(conf)

  override def deployAll(driverSettings: DriverSettings) = true
}

object FileSystemDriver {
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
  val checksums = HashMap[String,String]()
  
  def lookup(file: String) : Option[String]  = {
    checksums.get(file)
  }
  
  def cache(file: String, checksum: String) : String = {
    checksums.put(file, checksum)
    checksum
  }
  
  def clear() {
    checksums.clear()
  }
 
}