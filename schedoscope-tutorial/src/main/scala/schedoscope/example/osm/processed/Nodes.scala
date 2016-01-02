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

import brickhouse.udf.collect.CollectUDAF
import org.schedoscope.dsl.{ Parameter, View }
import org.schedoscope.dsl.storageformats.Parquet
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.{ insertInto, queryFromResource, withFunctions }
import org.schedoscope.dsl.views.{ Id, JobMetadata, MonthlyParameterization, PointOccurrence }
import schedoscope.example.osm.Globals._
import schedoscope.example.osm.stage.NodeTags

case class Nodes(
  year: Parameter[String],
  month: Parameter[String]) extends View
    with MonthlyParameterization
    with Id
    with PointOccurrence
    with JobMetadata {

  val version = fieldOf[Int]("OSM version - ignored")
  val userId = fieldOf[Int]("OSM user ID - ignored")
  val longitude = fieldOf[Double]("Longitude of the node")
  val latitude = fieldOf[Double]("Latitude of the node")
  val geohash = fieldOf[String]("A geoencoded area string")
  val tags = fieldOf[Map[String, String]]("A map of tags")

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

  materializeOnce
}
