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
import schedoscope.example.osm.stage.WayNodes
import schedoscope.example.osm.stage.WayTags

case class Ways(
//  year: Parameter[String] = today._1,
//  month: Parameter[String] = today._2
  ) extends View
    with Id
    with PointOccurrence
    with JobMetadata {
  
  val version = fieldOf[Int]
  val user_id = fieldOf[Int]
  val tags = fieldOf[Map[String,String]]
  val nodes = fieldOf[List[String]]

  dependsOn { () =>
    Seq(
      schedoscope.example.osm.stage.Ways(),
      WayNodes(),
      WayTags())
  }

  transformVia(() =>
    HiveTransformation(
      insertInto(
        this,
        queryFromResource("hiveql/processed/insert_ways.sql"),
        settings = Map("parquet.compression" -> "GZIP")
      ), withFunctions(this, Map("collect" -> classOf[CollectUDAF])))
    .configureWith(defaultHiveQlParameters(this)))

  comment("View of ways, their referenced nodes and tags")

  storedAs(Parquet())
}
