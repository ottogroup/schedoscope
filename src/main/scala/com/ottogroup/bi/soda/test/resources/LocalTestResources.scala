package com.ottogroup.bi.soda.test.resources

import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties

import scala.Array.canBuildFrom

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEAUXJARS
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALMODEAUTO
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALMODEMAXBYTES
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALMODEMAXINPUTFILES
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE

import net.lingala.zip4j.core.ZipFile

class LocalTestResources extends TestResources {
  setupLocalHadoop()

  def hiveSiteXmlPath = "target/test-classes/hive-site.xml"
  def dependenciesDir = "deploy/dependencies"

  var cachedWarehouseDir: String = null
  override def hiveWarehouseDir: String = {
    if (cachedWarehouseDir == null) {
      val dir = Paths.get("target/hive-warehouse").toAbsolutePath()
      if (Files.exists(dir)) {
        FileUtils.deleteDirectory(dir.toFile())
      }
      val d = Files.createDirectory(dir).toString.replaceAll("\\\\", "/")

      cachedWarehouseDir = new Path("file:///", d).toString()
    }
    cachedWarehouseDir
  }

  override def connection = {
    val c = hiveConf
    super.connection
  }

  var cachedHiveConf: HiveConf = null
  override def hiveConf: HiveConf = {
    if (cachedHiveConf == null) {
      // we don't directly instantiate a new HiveConf(), because then hive-site.xml
      // would be loaded from classpath too early (we must make sure to write 
      // hive-site.xml BEFORE it is loaded the first time)
      val conf = new Properties()
      conf.put(METASTOREWAREHOUSE.toString, hiveWarehouseDir)
      conf.put(LOCALMODEAUTO.toString, "true")
      conf.put(METASTORECONNECTURLKEY.toString, "jdbc:derby:memory:metastore_db;create=true")
      conf.put(HIVEAUXJARS.toString, compiledClassesPath())
      conf.put(LOCALMODEMAXINPUTFILES.toString, "20")
      conf.put(LOCALMODEMAXBYTES.toString, "1342177280L")
      val props = conf.stringPropertyNames().toArray().map(p => s"<property><name>${p.toString}</name><value>${conf.getProperty(p.toString)}</value></property>").mkString("\n")
      Files.write(Paths.get(hiveSiteXmlPath), ("<configuration>\n" + props + "\n</configuration>").getBytes());

      cachedHiveConf = new HiveConf()
    }

    cachedHiveConf
  }

  override def jdbcClass = "org.apache.hadoop.hive.jdbc.HiveDriver"

  override def jdbcUrl = "jdbc:hive://"

  override def remoteTestDirectory: String = new Path("file:///", Paths.get("target").toAbsolutePath().toString).toString // TODO

  var cachedFileSystem: FileSystem = null
  override def fileSystem: FileSystem = {
    if (cachedFileSystem == null)
      cachedFileSystem = FileSystem.getLocal(new Configuration())

    cachedFileSystem
  }

  override def namenode = "file:///"

  def compiledClassesPath() = {
    val classPathMembers = this.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs.map { _.toString() }.distinct
    val nonJarClassPathMembers = classPathMembers.filter { !_.endsWith(".jar") }.toList
    nonJarClassPathMembers.map(_.replaceAll("file:", "")).mkString(",")
  }

  def setupLocalHadoop() {
    if (System.getenv("HADOOP_HOME") == null) {
      throw new RuntimeException("HADOOP_HOME must be set!")
    }
    val classPathMembers = this.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs.map { _.toString() }.distinct
    val jarClassPathMembers = classPathMembers.filter { _.endsWith(".jar") }.toList
    val launcherJars = jarClassPathMembers.filter { _.contains("hadoop-launcher") }
    val hadoopHome = new File(System.getenv("HADOOP_HOME"))

    if (launcherJars.size == 1 && !hadoopHome.exists) {
      hadoopHome.mkdirs
      new ZipFile(launcherJars.head.replaceAll("file:", "")).extractAll(hadoopHome.toString)
      FileUtil.chmod(hadoopHome.toString, "777", true)
    }

    val hadoopLibDir = new File(hadoopHome.toString() + File.separator + "lib")
    if (hadoopLibDir.exists)
      FileUtils.deleteDirectory(hadoopLibDir)
    hadoopLibDir.mkdir

    val jarCopyOperations = jarClassPathMembers.foldLeft(List[(File, File)]()) {
      case (jarCopies, jarFile) =>
        ((new File(new URL(jarFile).toURI()),
          new File(new Path(System.getenv("HADOOP_HOME"), "lib" + File.separator + new Path(jarFile).getName).toString)) :: jarCopies)
    }

    jarCopyOperations.foreach { case (source, target) => { FileUtils.copyFile(source, target) } }
  }
}
