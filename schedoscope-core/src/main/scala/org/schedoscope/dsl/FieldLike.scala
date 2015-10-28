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
 * Base class for field-like entities, i.e., fields and parameters. Field-likes have a name and a type.
 */
abstract class FieldLike[T: Manifest] extends Named {

  /**
   * Type of the field-like
   */
  val t = manifest[T]

  var assignedStructure: Option[Structure] = None

  /**
   * Assign the field-like to a structure (or view)
   */
  def assignTo(s: Structure) {
    assignedStructure = Some(s)
  }

  /**
   * Naming base of field-likes is the name of the field or parameter in the structure it is assigned to.
   * If not yet assigned, the naming base is the type name of the field-like.
   */
  override def namingBase = assignedStructure match {
    case Some(s) => s.nameOf(this).getOrElse(t.runtimeClass.getSimpleName)
    case None    => t.runtimeClass.getSimpleName
  }
}

/**
 * Trait to support privacy sensitive field-likes.
 */
trait PrivacySensitive {
  var isPrivacySensitive = false
}

/**
 * Trait for value-carrying field-likes, i.e., parameters.
 */
trait ValueCarrying[T] {
  /**
   * The value of a value-carrying field-like. Implemented as an option as the value might not be set.
   */
  var v: Option[T] = None
}

/**
 * A field-like capturing view fields. Fields have an orderWeight determining their ordering and can override the
 * namingBase inherited from FieldLike.
 */
case class Field[T: Manifest](orderWeight: Long, nameOverride: String) extends FieldLike[T] with PrivacySensitive {
  override def namingBase = if (nameOverride != null) nameOverride else super.namingBase
}

/**
 * Helpers for fields-
 */
object Field {

  /**
   * Used to assign fields values in the test framework.
   */
  def v[T](f: Field[T], v: T) = (f, v)
}

/**
 * A field-like capturing view parameters (partitioning parameters). Parameters have a orderWeight determining their ordering
 * and can carry a value.
 */
case class Parameter[T: Manifest](orderWeight: Long) extends FieldLike[T] with ValueCarrying[T] with PrivacySensitive {
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