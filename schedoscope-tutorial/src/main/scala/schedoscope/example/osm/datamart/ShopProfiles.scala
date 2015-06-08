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