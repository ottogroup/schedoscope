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

import org.schedoscope.dsl.View

case class SeqTransformation[T1 <: Transformation, T2 <: Transformation](firstThisTransformation: T1, thenThatTransformation: T2, firstTransformationIsDriving: Boolean = true) extends Transformation {

  def name = "seq"

  override def forView(v: View) = {
    firstThisTransformation.forView(v)
    thenThatTransformation.forView(v)
    super.forView(v)
  }

  override def fileResourcesToChecksum =
    if (firstTransformationIsDriving)
      firstThisTransformation.fileResourcesToChecksum
    else
      firstThisTransformation.fileResourcesToChecksum ++ thenThatTransformation.fileResourcesToChecksum

  override def stringsToChecksum =
    if (firstTransformationIsDriving)
      firstThisTransformation.stringsToChecksum
    else
      firstThisTransformation.stringsToChecksum ++ thenThatTransformation.stringsToChecksum

  override def checksum: String =
    if (firstTransformationIsDriving)
      firstThisTransformation.checksum
    else
      super.checksum

  description = s"${firstThisTransformation.description} => ${thenThatTransformation.description}"
}