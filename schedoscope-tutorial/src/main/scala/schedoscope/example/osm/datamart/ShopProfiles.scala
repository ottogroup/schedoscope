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
import schedoscope.example.osm.processed.Nodes
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.views.DateParameterizationUtils.allMonths
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import schedoscope.example.osm.Globals._
import org.schedoscope.dsl.Parquet
import schedoscope.example.osm.datahub.Shops
import schedoscope.example.osm.datahub.Trainstations
import schedoscope.example.osm.datahub.Restaurants

case class ShopProfiles() extends View
  with Id
  with JobMetadata {

  val shop_name = fieldOf[String]
  val shop_type = fieldOf[String]
  val area = fieldOf[String]
  val cnt_competitors = fieldOf[Int]
  val cnt_restaurants = fieldOf[Int]
  val cnt_trainstations = fieldOf[Int]

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