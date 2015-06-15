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
package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.transformations.CopyFrom

case class Nodes() extends View {

  // Declare each column of the TSV-file
  val id = fieldOf[Long]
  val version = fieldOf[Int]
  val user_id = fieldOf[Int]
  val tstamp = fieldOf[String]
  val longitude = fieldOf[Double]
  val latitude = fieldOf[Double]

  transformVia(() => CopyFrom("classpath://osm-data/nodes.txt", this))

  comment("Stage View for data from file nodes.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}