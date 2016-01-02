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
package org.schedoscope.dsl.transformations

import org.schedoscope.dsl.{ FieldLike, Named }

/**
 * specifies a morphline transformation
 * @param imports list of morphline command classes to import
 * @param sampling sampling rate
 * @param anonymize list of fields that will be hashed during export
 * @param fields list of fields to export
 * @param fieldMapping mapping of field names to e.g. SQL column names
 *
 */
case class MorphlineTransformation(definition: String = "",
                                   imports: Seq[String] = List(),
                                   sampling: Int = 100,
                                   anonymize: Seq[Named] = List(),
                                   fields: Seq[Named] = List(),
                                   fieldMapping: Map[FieldLike[_], FieldLike[_]] = Map()) extends ExternalTransformation {
  override def name = "morphline"

  override def stringsToChecksum = List(definition)
}
