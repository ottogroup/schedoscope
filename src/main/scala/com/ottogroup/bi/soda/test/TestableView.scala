package com.ottogroup.bi.soda.test

import scala.collection.mutable.ListBuffer
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.Parameter._
import com.ottogroup.bi.soda.dsl.Field
import com.ottogroup.bi.soda.crate.DeploySchema
import com.ottogroup.bi.soda.bottler.driver.FileSystemDriver
import com.ottogroup.bi.soda.bottler.driver.OozieDriver
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.bottler.driver.HiveDriver
import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.Parameter
import com.ottogroup.bi.soda.dsl.ValueCarrying
import com.ottogroup.bi.soda.bottler.driver.Driver
import com.ottogroup.bi.soda.dsl.Structure
import com.ottogroup.bi.soda.test.resources.OozieTestResources
import com.ottogroup.bi.soda.dsl.transformations.oozie.OozieTransformation
import org.apache.hadoop.fs.Path
import com.ottogroup.bi.soda.dsl.transformations.sql.HiveTransformation

trait TestableView extends FillableView {}

trait test extends TestableView {

  var rowIdx = 0

  var driver: () => Driver = () => {
    this.transformation() match {
      case t: HiveTransformation => resources().hiveDriver
      case t: OozieTransformation => resources().oozieDriver
      // TODO: support other drivers
    }
  }

  val deps = ListBuffer[View with rows]()

  def then() {
    then(sortedBy = null)
  }

  def then(sortedBy: FieldLike[_]) {
    println("Creating empty target view")
    deploySchema()

    deps.map(d => {
      println("Creating base view " + d.n)
      d.write()
    })

    println("Deploying workflows, if needed")
    val trans = this.transformation()
    trans match {
      case ot: OozieTransformation => deployWorkflow(ot)
      case ht: HiveTransformation => deployFunctions(ht)
      case _ => None
    }

    println("Starting transformation")
    driver().runAndWait(trans)
    println("Populating results transformation, adding partition")
    // FIXME: some transformations may create the partition by themselves?
    val part = resources().bottler.createPartition(this)
    println("Added partition: " + part.getSd.getLocation)
    populate(sortedBy)

  }

  def basedOn(d: View with rows*) {
    d.map(el => el.resources = () => resources())
    deps ++= d
  }

  def v[T](f: FieldLike[T]): T = {
    if (rs(rowIdx).get(f.n).isEmpty)
      None.asInstanceOf[T]
    else
      rs(rowIdx).get(f.n).get.asInstanceOf[T]
  }

  def vs(f: FieldLike[_]): Array[(FieldLike[_], Any)] = {
    rs(rowIdx).get(f.n).get.asInstanceOf[Array[(FieldLike[_], Any)]]
  }

  def path(f: Any*): Array[(FieldLike[_], Any)] = {
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
    currObj.asInstanceOf[Array[(FieldLike[_], Any)]]
  }

  def withConfiguration(k: String, v: Any) {
    configureTransformation(k, v)
  }

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
      println("UPLOADED " + src + " to " + target)
      configureTransformation(prop, target.toString.replaceAll("^file:/", "file:///"))
    })
  }

  def row(fields: Unit*) {
    rowIdx = rowIdx + 1
  }

  override def rowId(): String = {
    rowIdPattern.format(rowIdx)
  }
}

trait clustertest extends test {
  val otr = new OozieTestResources()
  resources = () => otr
  def cluster = () => otr.mo
}
