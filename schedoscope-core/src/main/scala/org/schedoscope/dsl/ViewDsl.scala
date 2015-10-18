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
import org.schedoscope.dsl.storageformats._
trait ViewDsl extends StructureDsl {

  def dependsOn[V <: View: Manifest](dsf: () => Seq[V]): Unit

  def dependsOn[V <: View: Manifest](df: () => V): () => V

  def transformVia(ft: () => Transformation): Unit

  def comment(aComment: String): Unit

  def storedAs(f: StorageFormat, additionalStoragePathPrefix: String = null, additionalStoragePathSuffix: String = null): Unit

  def asTableSuffix[P <: Parameter[_]](p: P): P

  def privacySensitive[P <: PrivacySensitive](ps: P): P = {
    ps.isPrivacySensitive = true
    ps
  }
  
  def materializeOnce: Unit
}
