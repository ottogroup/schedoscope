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
  * They have an optional comment and can be privacy-sensitive.
  */
abstract class FieldLike[T: Manifest] extends Named {

  /**
    * Type of the field-like
    */
  val t = manifest[T]

  /**
    * Is the field privacy-sensitive?
    */
  var isPrivacySensitive = false

  /**
    * The structure or view a field's assigned to.
    */
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
    case None => t.runtimeClass.getSimpleName
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case f: FieldLike[T] => assignedStructure -> f.assignedStructure match {
      case (Some(struct1), Some(struct2)) => struct1.nameOf(this) == struct2.nameOf(f)
      case _ => false
    }
  }

  override def hashCode(): Int = assignedStructure match {
    case Some(struct) => struct.nameOf(this).hashCode()
    case _ => super.hashCode()
  }

  override def toString: String = assignedStructure match {
    case Some(view: View) => s"${view.tableName}.${view.nameOf(this).getOrElse("")}"
    case _ => namingBase
  }
}