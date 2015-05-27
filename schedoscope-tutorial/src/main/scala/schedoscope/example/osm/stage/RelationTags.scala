package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.transformations.CopyFrom
import org.schedoscope.dsl.TextFile

case class RelationTags() extends View {
  
  val relation_id = fieldOf[String]
  val key = fieldOf[String]
  val value = fieldOf[String]

  transformVia(() => CopyFrom("classpath://osm-data/relation_tags.txt", this))

  comment("Stage View for data from file relation_tags.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}
