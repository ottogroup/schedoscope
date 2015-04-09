package com.ottogroup.bi.soda.test

import java.io.File
import java.io.OutputStreamWriter
import java.net.URI

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.ResourceType
import org.apache.hadoop.hive.metastore.api.ResourceUri

import com.ottogroup.bi.soda.crate.ddl.HiveQl
import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.OozieTransformation
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.test.resources.TestResources

trait FillableView extends View with rows {}

trait rows extends View {
  env = "test"

  val localTestResources = new LocalTestResources()

  var resources: () => TestResources = () => localTestResources

  val rs = new ListBuffer[Map[String, Any]]()

  var allowNullFields: Boolean = false

  def rowIdPattern = "%04d"

  // prefix location 
  locationPathBuilder = (env: String) => resources().hiveWarehouseDir + ("/hdp/" + env.toLowerCase() + "/" + module.replaceFirst("app", "applications")).replaceAll("_", "/") + (if (additionalStoragePathPrefix != null) "/" + additionalStoragePathPrefix else "") + "/" + n + (if (additionalStoragePathSuffix != null) "/" + additionalStoragePathSuffix else "")
  // unify storage format
  storedAs(localTestResources.textStorage)

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

  def deployWorkflow(wf: OozieTransformation) = {
    val fs = resources().fileSystem
    val dest = new Path(resources().namenode + new URI(wf.workflowAppPath).getPath)

    if (!fs.exists(dest))
      fs.mkdirs(dest)

    // FIXME: make source path configurable, recursive upload
    val srcFilesFromMain = if (new File(s"src/main/resources/oozie/${wf.bundle}/${wf.workflow}").listFiles() == null)
      Array[File]()
    else
      new File(s"src/main/resources/oozie/${wf.bundle}/${wf.workflow}").listFiles()

    val srcFilesFromTest = if (new File(s"src/test/resources/oozie/${wf.bundle}/${wf.workflow}").listFiles() == null)
      Array[File]()
    else
      new File(s"src/test/resources/oozie/${wf.bundle}/${wf.workflow}").listFiles()

    (srcFilesFromMain ++ srcFilesFromTest).map(f => {
      val src = new Path("file:///" + f.getAbsolutePath)
      fs.copyFromLocalFile(src, dest)
    })

    val files = fs.listFiles(dest, true)
    
    // create copy of workflow with adapted workflow app path
    new OozieTransformation(wf.bundle, wf.workflow, dest.toString, wf.c).configureWith(wf.configuration.toMap)
  }

  def deployFunctions(ht: HiveTransformation) = {
    ht.udfs.map(f => {
      val jarFile = Class.forName(f.getClassName).getProtectionDomain.getCodeSource.getLocation.getFile
      val jarResource = new ResourceUri(ResourceType.JAR, jarFile)
      f.setResourceUris(List(jarResource))
    })
    ht
  }

  def deploySchema() {
    val d = resources().crate
    if (!d.schemaExists(this)) {
      d.dropAndCreateTableSchema(this)
    }
  }

  def writeData() {
    val d = resources().crate
    val partitionFilePath = if (this.isPartitioned())
      new Path(new URI(d.createPartition(this).getSd.getLocation).getPath)
    else
      new Path(this.fullPath)

    val partitionFile = new Path(partitionFilePath, "00000")
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