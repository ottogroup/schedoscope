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
package org.schedoscope.dsl

import scala.Array.canBuildFrom
import scala.collection.mutable.ListBuffer
import java.util.concurrent.ConcurrentHashMap
import java.lang.reflect.Method

abstract class Structure extends StructureDsl with Named {
  override def namingBase = this.getClass().getSimpleName()

  private val fieldOrder = ListBuffer[Field[_]]()

  def registerField(f: Field[_]) {
    fieldOrder += f
    f.assignTo(this)
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

  lazy val fieldLikeGetters =
    this.getClass
      .getMethods()
      .filter { _.getParameterTypes().length == 0 }
      .filter { !_.getName().contains("$") }
      .filter { m => classOf[FieldLike[_]].isAssignableFrom(m.getReturnType()) }

  def nameOf[F <: FieldLike[_]](f: F) =
    fieldLikeGetters
      .filter { _.getReturnType().isAssignableFrom(f.getClass()) }
      .filter { _.invoke(this) eq f }
      .map { _.getName() }
      .headOption
}
