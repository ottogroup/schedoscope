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

import java.io.OutputStreamWriter
import java.net.URI

import org.apache.hadoop.fs.Path
import org.schedoscope.dsl.{FieldLike, Named, View}
import org.schedoscope.test.resources.{LocalTestResources, TestResources}

import scala.collection.mutable.ListBuffer

/**
  * This trait allows the view to be filled with data and written into a
  * hive partition.
  */
trait WritableView extends View {

  env = "test"

  var resources: TestResources = new LocalTestResources()

  val rowData = new ListBuffer[Map[String, Any]]()

  var allowNullFields: Boolean = false

  var fileName = "00000"

  override def namingBase = this.getClass.getSuperclass.getSimpleName()

  moduleNameBuilder = () => Named.camelToLowerUnderscore(getClass().getSuperclass.getPackage().getName()).replaceAll("[.]", "_")

  // overrides (to enable correct table/database names, otherwise $$anonFunc...)
  tablePathBuilder = (env: String) => resources.hiveWarehouseDir + ("/hdp/" + env.toLowerCase() + "/" + module.replaceFirst("app", "applications")).replaceAll("_", "/") + (if (additionalStoragePathPrefix != null) "/" + additionalStoragePathPrefix else "") + "/" + n + (if (additionalStoragePathSuffix != null) "/" + additionalStoragePathSuffix else "")

  // unify storage format
  storedAs(resources.textStorage)

  /**
    * Inserts a row to this field. If columns are left out, they are either set to null or filled with random data.
    *
    * @param row List of tuples (FieldLike,Any) to specify column (fieldlike) and their value
    */
  def set(row: (FieldLike[_], Any)*) {
    val m = row.map(f => f._1.n -> f._2).toMap[String, Any]
    rowData.append(fields.map(f => {
      if (m.contains(f.n)) f.n -> m(f.n) else f.n -> nullOrRandom(f, rowData.size)
    }).toMap[String, Any])
  }

  /**
    * Generates a new rowId for the current row.
    */
  def rowId(): String = {
    import WritableView._
    rowIdPattern.format(rowData.size)
  }

  /**
    * Returns the number of rows in this view
    */
  def numRows(): Int = {
    rowData.size
  }

  /**
    * Fills this view with data from hive, potentially sorted by a column
    *
    * @param orderedBy the optional FieldLike for the column to sort by
    */
  def populate(orderedBy: Option[FieldLike[_]]) {
    val db = resources.database
    rowData.clear()
    rowData.appendAll(db.selectView(this, orderedBy))
  }

  def createViewTable() {
    val d = resources.crate
    if (!d.schemaExists(this)) {
      d.dropAndCreateTableSchema(this)
    }
  }

  def writeData() {
    val d = resources.crate
    val partitionFilePath = if (this.isPartitioned())
      new Path(new URI(d.createPartition(this).getSd.getLocation).getPath)
    else
      new Path(this.fullPath)

    val partitionFile = new Path(partitionFilePath, fileName)
    val fs = resources.fileSystem
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

  /**
    * Changes the name of the file the table is
    * written to.
 *
    * @param name
    */
  def withFileName(name: String): Unit = {
    fileName = name
  }

  private def nullOrRandom(f: FieldLike[_], i: Int) = {
    import WritableView._
    if (allowNullFields) "\\N" else FieldSequentialValue.get(f, rowData.size, rowIdPattern)
  }
}

object WritableView {
  def rowIdPattern = "%04d"
}

/**
  * Syntactic sugar
  */
trait rows extends WritableView
