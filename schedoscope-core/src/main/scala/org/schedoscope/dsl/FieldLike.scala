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

abstract class FieldLike[T: Manifest] extends Named {
  val t = manifest[T]

  var deferredNamingBase = () => t.runtimeClass.getSimpleName

  def assignTo(s: Structure) {
    deferredNamingBase = () => s.nameOf(this).getOrElse(t.runtimeClass.getSimpleName)
  }

  override def namingBase = deferredNamingBase()

  def <=(v: T) = (this, v)
}

trait PrivacySensitive {
  var isPrivacySensitive = false
}

trait ValueCarrying[T] {
  var v: Option[T] = None
}

case class Field[T: Manifest](orderWeight: Long, nameOverride: String) extends FieldLike[T] with PrivacySensitive {
  override def namingBase = if (nameOverride != null) nameOverride else super.namingBase
}

object Field {
  def v[T](f: Field[T], v: T) = f <= v
}

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

object Parameter {
  private var parameterCount = 0L

  def newCount = this.synchronized {
    parameterCount += 1
    parameterCount
  }

  def apply[T: Manifest](): Parameter[T] = {
    val parameter = new Parameter[T](newCount)
    parameter
  }

  def p[T: Manifest](v: Parameter[T]): Parameter[T] = p(v.v.get)

  def p[T: Manifest](v: T): Parameter[T] = {
    val f = Parameter[T]
    f.v = Some(v)
    f
  }
}