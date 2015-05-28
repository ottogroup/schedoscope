package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.CopyFrom
import org.schedoscope.dsl.TextFile

case class WayTags() extends View {
  
  val way_id = fieldOf[Long]
  val key = fieldOf[String]
  val value = fieldOf[String]

  transformVia(() => CopyFrom("classpath://osm-data/way_tags.txt", this))

  comment("Stage View for data from file way_tags.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}
