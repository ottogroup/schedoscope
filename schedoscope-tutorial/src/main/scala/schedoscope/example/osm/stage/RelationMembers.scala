package schedoscope.example.osm.stage

import org.schedoscope.dsl.View
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.transformations.CopyFrom

case class RelationMembers() extends View {

  val relation_id = fieldOf[String]
  val member_id = fieldOf[String]
  val member_type = fieldOf[String]
  val member_role = fieldOf[String]
  val sequence_id = fieldOf[Int]

  transformVia(() => CopyFrom("classpath://osm-data/relation_members.txt", this))

  comment("Stage View for data from file relation_members.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}
