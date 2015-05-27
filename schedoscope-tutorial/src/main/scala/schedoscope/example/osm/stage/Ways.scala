package schedoscope.example.osm.stage

import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.View
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.views.PointOccurrence
import org.schedoscope.dsl.transformations.CopyFrom

case class Ways() extends View
    with Id
    with PointOccurrence {

  val version = fieldOf[Int](1002)
  val user_id = fieldOf[Int](1001)
  val changeset_id = fieldOf[Long](999)

  transformVia(() => CopyFrom("classpath://osm-data/ways.txt", this))

  comment("Stage View for data from file ways.txt")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}