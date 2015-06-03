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

  dependsOn { () =>
    Seq(
      NodesWithGeohash(),
      NodeTags())
  }

  transformVia(() =>
    HiveTransformation(
        insertInto(
        this,
        queryFromResource("hiveql/processed/insert_nodes.sql"),
        settings = Map("parquet.compression" -> "GZIP")
        ), withFunctions(this, Map("collect" -> classOf[CollectUDAF])))
      .configureWith(defaultHiveQlParameters(this)))

  comment("View of nodes with tags and geohash")

  storedAs(Parquet())
}