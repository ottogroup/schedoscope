package com.ottogroup.bi.soda.test.resources

import org.apache.hadoop.hive.conf.HiveConf
import java.sql.Connection
import java.sql.DriverManager
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import com.ottogroup.bi.soda.test.Database
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import java.util.Properties
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.apache.commons.io.FileUtils
import java.nio.file.Files
import java.nio.file.Paths
import java.net.URLClassLoader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.io.File
import java.net.URL
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.model.ZipParameters

object LocalTestResources extends TestResources {
  
  setupLocalHadoop()

  val hiveLocalJdbcUrl = "jdbc:hive://"
  val hiveSiteXmlPath = "target/test-classes/hive-site.xml"
  val dependenciesDir = "deploy/dependencies"

  override val hiveWarehouseDir: String = {
    val dir = Paths.get("target/hive-warehouse").toAbsolutePath()
    if (Files.exists(dir)) {
      FileUtils.deleteDirectory(dir.toFile())
    }
    val d = Files.createDirectory(dir).toString.replaceAll("\\\\", "/")
    new Path("file:///", d).toString()
  }

  override val hiveConf: HiveConf = {
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
    new HiveConf()
  }

  override val connection: Connection = {
    val c = hiveConf //make sure config is written before JDBC connection is established
    Class.forName(HIVE_JDBC_CLASS)
    DriverManager.getConnection(hiveLocalJdbcUrl, "", "")
  }

  override val localTestDirectory: String = "" // TODO

  override val remoteTestDirectory: String = new Path("file:///", Paths.get("target").toAbsolutePath().toString).toString // TODO

  override val fileSystem: FileSystem = FileSystem.getLocal(new Configuration())

  override val fileSystemDriver: FileSystemDriver = null // TODO

  override val oozieDriver: OozieDriver = null // TODO

  override val metastoreClient: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)
  override val database: Database = new Database(connection, hiveLocalJdbcUrl)
  override val bottler: DeploySchema = DeploySchema(metastoreClient, connection)
  override val hiveDriver: HiveDriver = new HiveDriver(connection)
  
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
    val launcherJars =  jarClassPathMembers.filter { _.contains("hadoop-launcher") }    
    val hadoopHome = new File(System.getenv("HADOOP_HOME"))     
    
    if (launcherJars.size == 1 && !hadoopHome.exists) {    
        hadoopHome.mkdirs      
        val zip = new ZipFile(launcherJars.head.replaceAll("file:", ""))
        zip.extractAll(hadoopHome.toString)
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

    jarCopyOperations.foreach { case (source, target) => {FileUtils.copyFile(source, target) }}
  }  
}
