package com.ottogroup.bi.soda.test

import scala.collection.mutable.ListBuffer
import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.TextFile
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.crate.ddl.HiveQl
import java.util.Date
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.dsl.Named
import com.ottogroup.bi.soda.dsl.Structure
import scala.collection.mutable.HashMap
import com.ottogroup.bi.soda.test.resources.TestResources
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import org.apache.hadoop.fs.Path
import java.net.URI
import java.io.OutputStreamWriter
import java.io.File
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import com.ottogroup.bi.soda.dsl.TextFile
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation
import org.apache.hadoop.hive.metastore.api.ResourceUri
import org.apache.hadoop.hive.metastore.api.ResourceType
import collection.JavaConversions._

trait FillableView extends View with rows {}

trait rows extends View {

  env = "test"

  var resources: () => TestResources = () => LocalTestResources

  val rs = new ListBuffer[Map[String, Any]]()

  var allowNullFields: Boolean = false

  def rowIdPattern = "%04d"

  // prefix location 
  locationPathBuilder = (env: String) => resources().hiveWarehouseDir + ("/hdp/" + env.toLowerCase() + "/" + module.replaceFirst("app", "applications")).replaceAll("_", "/") + (if (additionalStoragePathPrefix != null) "/" + additionalStoragePathPrefix else "") + "/" + n + (if (additionalStoragePathSuffix != null) "/" + additionalStoragePathSuffix else "")
  // unify storage format
  storageFormat match {
    case f: TextFile => None
    case _ => storedAs(LocalTestResources.textStorage)
  }
  // overrides (to enable correct table/database names, otherwise $$anonFunc...) 
  override def namingBase = this.getClass.getSuperclass.getSimpleName()
  override def getCanonicalClassname = this.getClass.getSuperclass.getCanonicalName
  moduleNameBuilder = () => this.getClass().getSuperclass.getPackage().getName()

  def set(row: (FieldLike[_], Any)*) {
    val m = row.map(f => f._1.n -> f._2).toMap[String, Any]
    rs.append(fields.map(f => { if (m.contains(f.n)) f.n -> m(f.n) else f.n -> nullOrRandom(f, rs.size) }).toMap[String, Any])
  }

  def numRows(): Int = {
    rs.size
  }

  def rowId(): String = {
    rowIdPattern.format(rs.size)
  }

  def populate(s: FieldLike[_]) {
    rs.clear()
    rs.appendAll(resources().database.selectForView(this, s))
  }

  def write() {
    deploySchema()
    writeData()
  }

  def deployWorkflow(wf: OozieTransformation) {
    val fs = resources().fileSystem
    val dest = new Path(wf.workflowAppPath)
    if (!fs.exists(dest))
      fs.mkdirs(dest)
    // FIXME: make source path configurable
    new File(s"src/main/resources/oozie/${wf.bundle}/${wf.workflow}")
      .listFiles()
      .filter(f => f.isFile)
      .map(f => {
        val src = new Path(f.getAbsolutePath)
        fs.copyFromLocalFile(src, new Path(dest, f.getName))
      })
  }

  def deployFunctions(ht: HiveTransformation) {
    ht.functionDefs.map(f => {
      val jarFile = Class.forName(f.getClassName).getProtectionDomain.getCodeSource.getLocation.getFile
      val jarResource = new ResourceUri(ResourceType.JAR, jarFile)
      f.setResourceUris(List(jarResource))
    })
  }

  def deploySchema() {
    val d = resources().bottler
    println(HiveQl.ddl(this))
    if (!d.schemaExists(dbName, n, HiveQl.ddl(this))) {
      d.dropAndCreateTableSchema(dbName, n, HiveQl.ddl(this))
    }
  }

  def writeData() {
    val d = resources().bottler
    val partitionFilePath = if (!this.partitionParameters.isEmpty)
      new Path(new URI(d.createPartition(this).getSd.getLocation).getPath)
    else
      new Path(this.fullPath)

    val partitionFile = new Path(partitionFilePath, "00000")
    println("Writing data to " + partitionFile.toString())
    val fs = resources().fileSystem
    if (fs.exists(partitionFilePath))
      fs.delete(partitionFilePath, true)
    fs.mkdirs(partitionFilePath)
    val out = new OutputStreamWriter(fs.create(partitionFile))
    out.write(ViewSerDe.serialize(this))
    out.close
  }

  def withNullFields() {
    allowNullFields = true
  }

  private def nullOrRandom(f: FieldLike[_], i: Int) = {
    if (allowNullFields) "\\N" else FieldSequentialValue.get(f, rs.size, rowIdPattern)
  }
}