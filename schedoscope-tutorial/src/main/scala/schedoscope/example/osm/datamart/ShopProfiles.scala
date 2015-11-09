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
package schedoscope.example.osm.datamart

import org.schedoscope.dsl.View
import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.views.JobMetadata
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import schedoscope.example.osm.Globals._
import org.schedoscope.dsl.storageformats.Parquet
import schedoscope.example.osm.datahub.Shops
import schedoscope.example.osm.datahub.Trainstations
import schedoscope.example.osm.datahub.Restaurants

case class ShopProfiles() extends View
    with Id
    with JobMetadata {

  val shopName = fieldOf[String]("The name of the profiled shop")
  val shopType = fieldOf[String]("The type of shop, as given by OSM")
  val area = fieldOf[String]("A geoencoded area string")
  val cntCompetitors = fieldOf[Int]("The number of competitors in the area (shops of the same type)")
  val cntRestaurants = fieldOf[Int]("The number of restaurants in the area")
  val cntTrainstations = fieldOf[Int]("The number of trainstations in the area")

  dependsOn { () => Shops() }
  dependsOn { () => Restaurants() }
  dependsOn { () => Trainstations() }

  transformVia { () =>
    HiveTransformation(
      insertInto(
        this,
        queryFromResource("hiveql/datamart/insert_shop_profiles.sql"),
        settings = Map("parquet.compression" -> "GZIP")))
      .configureWith(defaultHiveQlParameters(this))
  }

  comment("Shop profiles showing number of nearby competitors, restaurants and trainstations for each shop")

  storedAs(Parquet())
}