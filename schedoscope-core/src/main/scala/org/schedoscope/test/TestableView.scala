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

import org.apache.hadoop.fs.Path
import org.schedoscope.dsl.{ FieldLike, Structure, View }
import org.schedoscope.dsl.transformations.{ FilesystemTransformation, HiveTransformation, MapreduceTransformation, OozieTransformation, PigTransformation, Transformation }
import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.test.resources.OozieTestResources
import scala.collection.mutable.ListBuffer
import org.schedoscope.dsl.transformations.SeqTransformation

trait TestableView extends FillableView {}

/**
 * This trait implements most of the schedoscope test DSL. it extends View
 * with methods to generate test data, execute local hive and assertions
 *
 */
trait test extends TestableView {
  var rowIdx = 0

  var driver: () => Driver[Transformation] = () => {
    this.transformation() match {
      case t: SeqTransformation[_,_]   => resources().seqDriver.asInstanceOf[Driver[Transformation]]
      case t: HiveTransformation       => resources().hiveDriver.asInstanceOf[Driver[Transformation]]
      case t: OozieTransformation      => resources().oozieDriver.asInstanceOf[Driver[Transformation]]
      case t: PigTransformation        => resources().pigDriver.asInstanceOf[Driver[Transformation]]
      case t: MapreduceTransformation  => resources().mapreduceDriver.asInstanceOf[Driver[Transformation]]
      case t: FilesystemTransformation => resources().fileSystemDriver.asInstanceOf[Driver[Transformation]]
    }
  }

  val deps = ListBuffer[View with rows]()

  /**
   * Execute the hive query in test on previously specified test fixtures
   */
  def `then`() {
    `then`(sortedBy = null)
  }

  /**
   * Execute the hive query in test on previously specified test fixtures.
   * Sort the table by Field
   */
  def `then`(sortedBy: FieldLike[_]) {
    deploySchema()

    deps.map(d => {
      d.write()
    })

    //
    // Patch export configurations to point to the test metastore with no kerberization.
    //
    configureExport("isKerberized", false)
    configureExport("kerberosPrincipal", "")
    configureExport("metastoreUri", resources().metastoreUri)
    
    val trans = this.transformation() match {
      case ot: OozieTransformation => deployWorkflow(ot)
      case ht: HiveTransformation  => deployFunctions(ht)
      case t: Transformation       => t
    }

    val d = driver()
    d.runAndWait(trans)
    // FIXME: some transformations may create the partition by themselves?
    if (this.isPartitioned()) {
      val part = resources().crate.createPartition(this)
    }
    populate(sortedBy)
  }

  /**
   * Adds dependencies for this view
   */
  def basedOn(d: View with rows*) {
    d.map(el => el.resources = () => resources())
    deps ++= d
  }

  /**
   * Get the value of a file in the current row
   *
   * @param f FieldLike to select the field (by name)
   *
   */
  def v[T](f: FieldLike[T]): T = {
    if (rs(rowIdx).get(f.n).isEmpty)
      None.asInstanceOf[T]
    else
      rs(rowIdx).get(f.n).get.asInstanceOf[T]
  }

  /**
   * Gett multiple values from a multi-valued field
   *
   */
  def vs(f: FieldLike[_]): Array[(FieldLike[_], Any)] = {
    rs(rowIdx).get(f.n).get.asInstanceOf[Array[(FieldLike[_], Any)]]
  }

  /**
   * Retrieves a Structure from a list of structures. Only one use case is
   * implemented: if a view has a fieldOf[List[Structure]], an element
   * of this list can be selected like this path(ListOfStructs, 1)
   *
   */
  def path(f: Any*): Map[String, Any] = {
    var currObj: Any = v(f.head.asInstanceOf[FieldLike[_]])
    f.tail.foreach(p => {
      p match {
        case i: Int =>
          currObj = currObj.asInstanceOf[List[Any]](i)
        case fl: FieldLike[_] => {
          if (classOf[Structure].isAssignableFrom(fl.t.runtimeClass)) {
            // TODO
          } else if (fl.t.runtimeClass == classOf[List[_]]) {
            // TODO
          } else if (fl.t.runtimeClass == classOf[Map[_, _]]) {
            // TODO
          }
        }
      }
    })
    currObj.asInstanceOf[Map[String, Any]]

  }

  /**
   * Configures the associated transformation with the given property (as
   * key value pair)
   */
  def withConfiguration(k: String, v: Any) {
    configureTransformation(k, v)
  }

  /**
   * Configures the associated transformation with the given property (as
   * multiple key value pairs)
   */
  def withConfiguration(c: (String, Any)*) {
    c.foreach(e => this.configureTransformation(e._1, e._2))
  }

  def withResource(res: (String, String)*) {
    res.foreach(r => {
      val prop = r._1
      val file = r._2
      val fs = resources().fileSystem
      val src = new Path(file)
      val target = new Path(s"${resources().remoteTestDirectory}/${src.getName}")
      if (fs.exists(target))
        fs.delete(target, false)
      fs.copyFromLocalFile(src, target)
      configureTransformation(prop, target.toString.replaceAll("^file:/", "file:///"))
    })
  }

  /**
   * Just increments the row index, new values that are injected by set
   * will be added to the next row
   */
  def row(fields: Unit*) {
    rowIdx = rowIdx + 1
  }

  /* (non-Javadoc)
   * @see org.schedoscope.test.rows#rowId()
  */
  override def rowId(): String = {
    rowIdPattern.format(rowIdx)
  }
}

/**
 * A test environment that is executed in a local minicluster
 */
trait clustertest extends test {
  val otr = OozieTestResources()
  resources = () => otr

  def cluster = () => otr.mo
}
