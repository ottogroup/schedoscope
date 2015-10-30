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

  /**
   * Return all fields in weight order.
   */
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
object Structure {
   case class TypedAny(v: Any, t: Manifest[_])
     implicit def t[V: Manifest](v: V) = TypedAny(v, manifest[V])
  def newStructure[V <: Structure: Manifest](structureClass: Class[V], parameterValues: TypedAny*): V = {
    val viewCompanionObjectClass = Class.forName(structureClass.getName() + "$")
    viewCompanionObjectClass.getConstructors().foreach( con=> println(con.toGenericString()))
    val viewCompanionConstructor = viewCompanionObjectClass.getDeclaredConstructor()
    viewCompanionConstructor.setAccessible(true)
    val viewCompanionObject = viewCompanionConstructor.newInstance()

    val applyMethods = viewCompanionObjectClass.getDeclaredMethods()
      .filter { _.getName() == "apply" }

    val viewConstructor = applyMethods
      .filter { apply =>
        val parameterTypes = apply.getGenericParameterTypes().distinct
        !((parameterTypes.length == 1) && (parameterTypes.head == classOf[Object]))
      }
      .head

    val parametersToPass = ListBuffer[Any]()
    val parameterValuesPassed = ListBuffer[TypedAny]()
    parameterValuesPassed ++= parameterValues


    for (constructorParameterType <- viewConstructor.getParameterTypes()) {
      var passedValueForParameter: TypedAny = null

      for (parameterValue <- parameterValuesPassed; if passedValueForParameter == null) {
        if (constructorParameterType.isAssignableFrom(parameterValue.t.erasure)) {
          passedValueForParameter = parameterValue
        }
      }

      if (passedValueForParameter != null) {
        parameterValuesPassed -= passedValueForParameter
      }

      parametersToPass += passedValueForParameter.v
    }

   viewConstructor.invoke(viewCompanionObject).asInstanceOf[V]
  }

}
