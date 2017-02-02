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

package org.schedoscope.lineage

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Schema.TableType
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.schedoscope.dsl.{Structure, View}

import scala.collection.JavaConverters._

/**
  * Table based on reflection over [[View]]s.
  * <p>
  * Uses the following field type conversions:
  * <table>
  *   <tr>
  *     <th>ScalaType</th>
  *     <th>RelDataType</th>
  *   </tr>
  *   <tr>
  *     <td>Byte</td>
  *     <td>SMALLINT</td>
  *   </tr>
  *   <tr>
  *     <td>Int</td>
  *     <td>INTEGER</td>
  *   </tr>
  *   <tr>
  *     <td>Long</td>
  *     <td>BIGINT</td>
  *   </tr>
  *   <tr>
  *     <td>Boolean</td>
  *     <td>BOOLEAN</td>
  *   </tr>
  *   <tr>
  *     <td>Double</td>
  *     <td>DOUBLE</td>
  *   </tr>
  *   <tr>
  *     <td>Float</td>
  *     <td>FLOAT</td>
  *   </tr>
  *   <tr>
  *     <td>String</td>
  *     <td>VARCHAR</td>
  *   </tr>
  *   <tr>
  *     <td>List[A]</td>
  *     <td>RelRecordType[A]<br></td>
  *   </tr>
  *   <tr>
  *     <td>Map[K,V]</td>
  *     <td>MapSqlType[K,V]</td>
  *   </tr>
  *   <tr>
  *     <td>Structure<br></td>
  *     <td>RelRecordType<br></td>
  *   </tr>
  *   <tr>
  *     <td>_<br></td>
  *     <td>ANY<br></td>
  *   </tr>
  * </table>
  *
  * @author Jan Hicken (jhicken)
  */
case class SchedoscopeTable(view: View) extends AbstractTable {
  override val getJdbcTableType: TableType = TableType.VIEW

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    typeFactory.createStructType(
      view.fieldsAndParameters.map(f => relDataTypeOf(f.t, typeFactory)).asJava,
      view.fieldsAndParameters.map(_.n).asJava
    )
  }

  private def relDataTypeOf(scalaType: Manifest[_], typeFactory: RelDataTypeFactory): RelDataType = {
    if (scalaType.runtimeClass == classOf[List[_]])
      typeFactory.createArrayType(
        relDataTypeOf(scalaType.typeArguments.head, typeFactory),
        -1
      )
    else if (scalaType.runtimeClass == classOf[Map[_, _]])
      typeFactory.createMapType(
        relDataTypeOf(scalaType.typeArguments.head, typeFactory),
        relDataTypeOf(scalaType.typeArguments(1), typeFactory)
      )
    else if (classOf[Structure].isAssignableFrom(scalaType.runtimeClass)) {
      val struct = scalaType.runtimeClass.newInstance().asInstanceOf[Structure]
      typeFactory.createStructType(
        struct.fields.map(f => relDataTypeOf(f.t, typeFactory)).asJava,
        struct.fields.map(_.n).asJava
      )
    }
    else if (scalaType == manifest[Byte]) typeFactory.createSqlType(SqlTypeName.SMALLINT)
    else if (scalaType == manifest[Int]) typeFactory.createSqlType(SqlTypeName.INTEGER)
    else if (scalaType == manifest[Long]) typeFactory.createSqlType(SqlTypeName.BIGINT)
    else if (scalaType == manifest[Boolean]) typeFactory.createSqlType(SqlTypeName.BOOLEAN)
    else if (scalaType == manifest[Double]) typeFactory.createSqlType(SqlTypeName.DOUBLE)
    else if (scalaType == manifest[Float]) typeFactory.createSqlType(SqlTypeName.FLOAT)
    else if (scalaType == manifest[String]) typeFactory.createSqlType(SqlTypeName.VARCHAR)
    else typeFactory.createSqlType(SqlTypeName.ANY)
  }
}
