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
package schedoscope.example.osm.datahub

import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.View
import org.schedoscope.dsl.storageformats.Parquet
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.{insertInto, queryFromResource}
import org.schedoscope.dsl.transformations.SparkTransformation.runOnSpark
import org.schedoscope.dsl.views.DateParameterizationUtils.allMonths
import org.schedoscope.dsl.views.{Id, JobMetadata}
import schedoscope.example.osm.Globals._
import schedoscope.example.osm.processed.Nodes

case class Restaurants() extends View
  with Id
  with JobMetadata {

  val restaurantName = fieldOf[String]("The name of a restaurant")
  val restaurantType = fieldOf[String]("The cuisine of a restaurant, as given by OSM.")
  val area = fieldOf[String]("A geoencoded area string")

  dependsOn { () =>
    for ((year, month) <- allMonths())
      yield Nodes(p(year), p(month)).affects(n => Seq(
        n.id -> id,
        n.tags -> restaurantName,
        n.tags -> restaurantType,
        n.geohash -> area
      ))
  }

  transformVia { () =>
      runOnSpark(
        HiveTransformation(
          insertInto(
            this,
            queryFromResource("hiveql/datahub/insert_restaurants.sql")))
          .configureWith(defaultHiveQlParameters(this))
      )
  }

  comment("View of restaurants")

  storedAs(Parquet())
}