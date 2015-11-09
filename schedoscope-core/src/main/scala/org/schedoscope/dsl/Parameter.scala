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

/**
 * A field-like capturing view parameters (partitioning parameters). Parameters have a orderWeight determining their ordering
 * and can carry a value.
 */
case class Parameter[T: Manifest](orderWeight: Long) extends FieldLike[T] {
  /**
   * The value assigned to a parameter.
   */
  var v: Option[T] = None
  
  override def equals(a: Any): Boolean = {
    if (a.getClass != this.getClass()) {
      return false
    }

    val p = a.asInstanceOf[Parameter[T]]

    (p.t == this.t) && (p.v == this.v)
  }

  override def hashCode(): Int = {
    t.hashCode + 7 * v.hashCode()
  }

  override def toString() = if (v.isDefined) s"Parameter(${v.get})" else super.toString
}

/**
 * Helper methods for parameters.
 */
object Parameter {
  private var parameterCount = 0L

  private def newCount = this.synchronized {
    parameterCount += 1
    parameterCount
  }

  /**
   * Constructor for creating parameters without a value.
   *
   * Because, syntactically, we would like to create parameters within the case class parameters of a view,
   * we sadly seem to need to funnel all parameter creation through here, such that they can receive a global
   * ordering despite not yet being assigned to a view at the time of their creation.
   */
  def apply[T: Manifest](): Parameter[T] = {
    val parameter = new Parameter[T](newCount)
    parameter
  }

  /**
   * Create a parameter for a value.
   */
  def p[T: Manifest](v: T): Parameter[T] = {
    val f = Parameter[T]
    f.v = Some(v)
    f
  }

  /**
   * Create a parameter out of an existing parameter, thereby assigning it a new order weight. When passing parameters
   * between views, they should be wrapped using this method to ensure correct parameter ordering.
   */
  def p[T: Manifest](v: Parameter[T]): Parameter[T] = p(v.v.get)
}