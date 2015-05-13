package org.schedoscope.dsl

import scala.Array.canBuildFrom
import scala.collection.mutable.ListBuffer

abstract class Structure extends StructureDsl with Named {
  var parentField: FieldLike[Structure] = null

  override def namingBase = this.getClass().getSimpleName()

  private val fieldOrder = ListBuffer[Field[_]]()

  def registerField(f: Field[_]) {
    fieldOrder += f
    f.structure = this
  }

  def fields = {
    val fieldsWithWeightsAndPosition = ListBuffer[(Long, Int, Field[_])]()

    for (i <- 0 until fieldOrder.length) {
      val field = fieldOrder(i)
      fieldsWithWeightsAndPosition.append((field.orderWeight, i, field))
    }

    fieldsWithWeightsAndPosition
      .sortWith { case ((w1, i1, _), (w2, i2, _)) => (w1 > w2) || ((w1 == w2) && (i1 < i2)) }
      .map { case (_, _, f) => f }.toSeq
  }

  def nameOf[P <: FieldLike[_]](p: P) =
    this.getClass().getMethods()
      .filter { _.getParameterTypes().length == 0 }
      .filter { !_.getName().contains("$") }
      .filter { _.getReturnType().isAssignableFrom(p.getClass()) }
      .filter { _.invoke(this) eq p }
      .map { _.getName() }
      .headOption
}