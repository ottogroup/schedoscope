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
package schedoscope.example.osm.processed

import org.schedoscope.dsl.View
import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.views.PointOccurrence
import org.schedoscope.dsl.views.JobMetadata
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import org.schedoscope.dsl.transformations.HiveTransformation.withFunctions
import org.schedoscope.dsl.Parquet
import schedoscope.example.osm.Globals._
import brickhouse.udf.collect.CollectUDAF
import schedoscope.example.osm.stage.NodeTags
import org.schedoscope.dsl.views.MonthlyParameterization
import org.schedoscope.dsl.Parameter

case class Nodes(
  year: Parameter[String],
  month: Parameter[String]) extends View
  with MonthlyParameterization
  with Id
  with PointOccurrence
  with JobMetadata {

  val version = fieldOf[Int]
  val user_id = fieldOf[Int]
  val longitude = fieldOf[Double]
  val latitude = fieldOf[Double]
  val geohash = fieldOf[String]
  val tags = fieldOf[Map[String, String]]

  dependsOn(() => NodesWithGeohash())
  dependsOn(() => NodeTags())

  transformVia(() =>
    HiveTransformation(
      insertInto(
        this,
        queryFromResource("hiveql/processed/insert_nodes.sql"),
        settings = Map("parquet.compression" -> "GZIP")), withFunctions(this, Map("collect" -> classOf[CollectUDAF])))
      .configureWith(defaultHiveQlParameters(this)))

  comment("View of nodes with tags and geohash")

  storedAs(Parquet())
}