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
package org.schedoscope.test

import org.schedoscope.dsl.{Structure, FieldLike}

import scala.collection.mutable.ListBuffer

trait AccessRowData {

  var rowIdx = 0
  val rowData: ListBuffer[Map[String, Any]]


  /**
    * Get the value of a file in the current row
    *
    * @param f FieldLike to select the field (by name)
    *
    */
  def v[T](f: FieldLike[T]): T = {
    if (rowData(rowIdx).get(f.n).isEmpty)
      None.asInstanceOf[T]
    else
      rowData(rowIdx).get(f.n).get.asInstanceOf[T]
  }

  /**
    * Get multiple values from a multi-valued field
    *
    */
  def vs(f: FieldLike[_]): Array[(FieldLike[_], Any)] = {
    rowData(rowIdx).get(f.n).get.asInstanceOf[Array[(FieldLike[_], Any)]]
  }

  /**
    * Just increments the row index, new values that are injected by set
    * will be added to the next row
    */
  def row(fields: Unit*) {
    rowIdx = rowIdx + 1
  }


  /* (non-Javadoc)
   * @see org.schedoscope.test.rows#rowId()
  */
  def rowId(): String = {
    WriteableView.rowIdPattern.format(rowIdx)
  }

  /**
    * Retrieves a Structure from a list of structures. Only one use case is
    * implemented: if a view has a fieldOf[List[Structure]], an element
    * of this list can be selected like this path(ListOfStructs, 1)
    *
    */
  def path(f: Any*): Map[String, Any] = {
    var currObj: Any = v(f.head.asInstanceOf[FieldLike[_]])
    f.tail.foreach(p => {
      p match {
        case i: Int =>
          currObj = currObj.asInstanceOf[List[Any]](i)
        case fl: FieldLike[_] => {
          if (classOf[Structure].isAssignableFrom(fl.t.runtimeClass)) {
            // TODO
          } else if (fl.t.runtimeClass == classOf[List[_]]) {
            // TODO
          } else if (fl.t.runtimeClass == classOf[Map[_, _]]) {
            // TODO
          }
        }
      }
    })
    currObj.asInstanceOf[Map[String, Any]]

  }

  /**
    * Returns the number of rows in this view
    */
  def numRows(): Int = {
    rowData.size
  }
}
