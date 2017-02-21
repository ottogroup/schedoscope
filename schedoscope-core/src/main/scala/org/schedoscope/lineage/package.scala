/*
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope

import org.schedoscope.dsl.transformations.{HiveTransformation, SeqTransformation}
import org.schedoscope.dsl.{FieldLike, View}

import scala.language.implicitConversions

/**
  * @author Jan Hicken (jhicken)
  */
package object lineage {
  /**
    * Maps [[org.schedoscope.dsl.FieldLike]]s to its dependencies
    */
  type DependencyMap = Map[FieldLike[_], FieldSet]
  /**
    * A set of [[org.schedoscope.dsl.FieldLike]]s
    */
  type FieldSet = Set[FieldLike[_]]

  /**
    * Wrapper class for a [[org.schedoscope.dsl.View]] to facilitate access to a specified [[org.schedoscope.dsl.transformations.HiveTransformation]].
    *
    * @param view a view
    */
  implicit class ViewWithHiveTransformation(val view: View) extends AnyVal {
    /**
      * Searches all transformations of a view specified by ```transformVia()``` for a [[org.schedoscope.dsl.transformations.HiveTransformation]].
      *
      * @return a HiveTransformation, if found
      */
    def hiveTransformation: Option[HiveTransformation] = view.transformation() match {
      case ht: HiveTransformation => Some(ht)
      case st: SeqTransformation[_, _] => getHiveTransformation(st)
      case _ => None
    }

    /**
      * Recursive helper function to handle the recursive [[org.schedoscope.dsl.transformations.SeqTransformation]] data structure.
      *
      * @param st a SeqTransformation
      * @return a HiveTransformation, if found
      */
    private def getHiveTransformation(st: SeqTransformation[_, _]): Option[HiveTransformation] =
    st.firstThisTransformation -> st.thenThatTransformation match {
      case (ht: HiveTransformation, _) => Some(ht)
      case (_, ht: HiveTransformation) => Some(ht)
      case (st: SeqTransformation[_, _], _) => getHiveTransformation(st)
      case (_, st: SeqTransformation[_, _]) => getHiveTransformation(st)
      case _ => None
    }
  }

  object NoHiveTransformationException extends Exception
}
