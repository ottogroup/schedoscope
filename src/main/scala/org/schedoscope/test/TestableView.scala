package org.schedoscope.test

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path

import com.ottogroup.bi.soda.bottler.driver.Driver
import org.schedoscope.dsl.FieldLike
import org.schedoscope.dsl.Structure
import com.ottogroup.bi.soda.dsl.Transformation
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.OozieTransformation
import com.ottogroup.bi.soda.dsl.transformations.HiveTransformation
import com.ottogroup.bi.soda.dsl.transformations.PigTransformation
import com.ottogroup.bi.soda.dsl.transformations.MapreduceTransformation
import com.ottogroup.bi.soda.test.resources.OozieTestResources

trait TestableView extends FillableView {}

trait test extends TestableView {
  var rowIdx = 0

  var driver: () => Driver[Transformation] = () => {
    this.transformation() match {
      case t: HiveTransformation => resources().hiveDriver.asInstanceOf[Driver[Transformation]]
      case t: OozieTransformation => resources().oozieDriver.asInstanceOf[Driver[Transformation]]
      case t: PigTransformation => resources().pigDriver.asInstanceOf[Driver[Transformation]]
      case t: MapreduceTransformation => resources().mapreduceDriver.asInstanceOf[Driver[Transformation]]
    }
  }

  val deps = ListBuffer[View with rows]()

  def then() {
    then(sortedBy = null)
  }

  def then(sortedBy: FieldLike[_]) {
    deploySchema()

    deps.map(d => {
      d.write()
    })

    val trans = this.transformation() match {
      case ot: OozieTransformation => deployWorkflow(ot)
      case ht: HiveTransformation => deployFunctions(ht)
      case t: Transformation => t
    }

    val d = driver()
    val rto = d.runTimeOut
    d.runAndWait(trans)
    // FIXME: some transformations may create the partition by themselves?
    if (this.isPartitioned()) {
      val part = resources().crate.createPartition(this)
    }
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
  val otr = OozieTestResources()
  resources = () => otr
  def cluster = () => otr.mo
}