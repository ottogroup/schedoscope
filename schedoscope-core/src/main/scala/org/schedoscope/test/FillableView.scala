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
package org.schedoscope.test

import java.io.{ File, OutputStreamWriter }
import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{ ResourceType, ResourceUri }
import org.schedoscope.dsl.{ FieldLike, Named, View }
import org.schedoscope.dsl.transformations.{ HiveTransformation, OozieTransformation }
import org.schedoscope.test.resources.{ LocalTestResources, TestResources }

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer

/**
 * A fillable View is a View with rows of data (a Table).
 */
trait FillableView extends View with rows {}

trait rows extends View {
  env = "test"

  val localTestResources = new LocalTestResources()

  var resources: () => TestResources = () => localTestResources

  val rs = new ListBuffer[Map[String, Any]]()

  var allowNullFields: Boolean = false

  def rowIdPattern = "%04d"

  override def namingBase = this.getClass.getSuperclass.getSimpleName()

  moduleNameBuilder = () => Named.camelToLowerUnderscore(getClass().getSuperclass.getPackage().getName()).replaceAll("[.]", "_")

  tablePathBuilder = (env: String) => resources().hiveWarehouseDir + ("/hdp/" + env.toLowerCase() + "/" + module.replaceFirst("app", "applications")).replaceAll("_", "/") + (if (additionalStoragePathPrefix != null) "/" + additionalStoragePathPrefix else "") + "/" + n + (if (additionalStoragePathSuffix != null) "/" + additionalStoragePathSuffix else "")

  // unify storage format
  storedAs(localTestResources.textStorage)

  // overrides (to enable correct table/database names, otherwise $$anonFunc...) 

  /**
   * Inserts a row to this field. If columns are left out, they are either set to null or filled with random data.
   *
   * @param row List of tuples (FieldLike,Any) to specify column (fieldlike) and their value
   */
  def set(row: (FieldLike[_], Any)*) {
    val m = row.map(f => f._1.n -> f._2).toMap[String, Any]
    rs.append(fields.map(f => {
      if (m.contains(f.n)) f.n -> m(f.n) else f.n -> nullOrRandom(f, rs.size)
    }).toMap[String, Any])
  }

  /**
   * Returns the number of rows in this view
   */
  def numRows(): Int = {
    rs.size
  }

  /**
   * Generates a new rowId for the current row.
   */
  def rowId(): String = {
    rowIdPattern.format(rs.size)
  }

  /**
   * Fills this view with data from hive, selects only column within s
   *
   * @param s FieldLike to specify the column to fill
   */
  def populate(s: FieldLike[_]) {
    rs.clear()
    rs.appendAll(resources().database.selectForView(this, s))
  }

  /**
   * Persists this view into local hive
   *
   */
  def write() {
    deploySchema()
    writeData()
  }

  /**
   * Deploys a local oozie workflow for oozie tests
   *
   */
  def deployWorkflow(wf: OozieTransformation) {
    val fs = resources().fileSystem
    val dest = new Path(resources().namenode + new URI(wf.workflowAppPath).getPath + "/")

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

    wf.workflowAppPath = dest.toString()
  }

  /**
   * Modifies a hivetransformation so that it will find locally deployed UDFS
   *
   */
  def deployFunctions(ht: HiveTransformation) {
    ht.udfs.foreach {
      f =>
        {
          val jarFile = Class.forName(f.getClassName).getProtectionDomain.getCodeSource.getLocation.getFile
          val jarResource = new ResourceUri(ResourceType.JAR, jarFile)
          f.setResourceUris(List(jarResource))
        }
    }
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
    val out = new OutputStreamWriter(fs.create(partitionFile), "UTF-8")
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