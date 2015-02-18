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
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveQl
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

class FileSystemDriver(val ugi:UserGroupInformation,conf:Configuration) extends Driver {

  def doAs(f: () => Boolean): Boolean = ugi.doAs(new PrivilegedAction[Boolean]() {
    def run(): Boolean = {
      f()
    }
  })

  def run(t: Transformation): String = {
    // TODO non-blocking implementation possible / wanted?
    ""
  }

  def runAndWait(t: Transformation): Boolean = {
    t match {
      case IfExists(path, op) => doAs(() => {
        if (FileSystem.get(uri(path), conf).exists(new Path(path)))
          runAndWait(op) else true
      })
      case IfNotExists(path, op) => doAs(() => {
        if (!FileSystem.get(uri(path), conf).exists(new Path(path)))
          runAndWait(op) else true
      })

      case Copy(from, to, recursive) => doAs(() => copy(from, to, recursive))
      case Move(from, to) => doAs(() => move(from, to))
      case Delete(path, recursive) => doAs(() => delete(path, recursive))
      case Touch(path) => doAs(() => touch(path))
    }
  }

  def copy(from: String, to: String, recursive: Boolean) = {

    val fromFS = FileSystem.get(uri(from), conf)
    val toFS = FileSystem.get(uri(to), conf)
    val files = listFiles(fromFS, from)
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

    val fromFS = FileSystem.get(uri(from), conf)
    val files = listFiles(fromFS, from)
    try {
      files.foreach(status => fromFS.delete(status.getPath(), recursive))
    } catch {
      case e: Throwable => false
    }
    true
  }

  def touch(path: String) = {
    val filesys = FileSystem.get(uri(path), conf)
    try {
      filesys.create(new Path(path))
    } catch {
      case e: Throwable => false
    }
    true
  }

  def move(from: String, to: String) = {
    val fromFS = FileSystem.get(uri(from), conf)
    val toFS = FileSystem.get(uri(to), conf)
    val files = listFiles(fromFS, from)
    try {
      FileUtil.copy(fromFS, FileUtil.stat2Paths(files), toFS, new Path(to), true, true, conf)
    } catch {
      case e: Throwable => false
    }
    true
  }

  private def listFiles(fs: FileSystem, path: String): Array[FileStatus] = {
    fs.globStatus(new Path(path))
  }

  private def uri(pathOrUri: String) =
    try {
      new URI(pathOrUri)
    } catch {
      case _: Throwable => new File(pathOrUri).toURI()
    }

}

object FileSystemDriver {
  def apply(ds:DriverSettings) = {
    val fsd = new FileSystemDriver(Settings().userGroupInformation,Settings().hadoopConf)
    fsd.driverSettings = ds
    fsd  
  }
  def apply(ugi:UserGroupInformation,conf:Configuration) =  new FileSystemDriver(ugi,conf)

}