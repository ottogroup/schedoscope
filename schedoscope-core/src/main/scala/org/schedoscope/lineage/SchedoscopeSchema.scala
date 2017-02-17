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

import java.util

import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.schedoscope.dsl.View

import scala.collection.JavaConverters._

/**
  * Schema based on reflection over [[org.schedoscope.dsl.View]]s.
  *
  * @author Jan Hicken (jhicken)
  */
case class SchedoscopeSchema(views: Traversable[View]) extends AbstractSchema {
  override def getTableMap: util.Map[String, Table] = views.map(
    v => v.n -> SchedoscopeTable(v)
  ).toMap[String, Table].asJava
}
