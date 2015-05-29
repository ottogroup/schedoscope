package schedoscope.example.osm.processed

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.schedoscope.test.rows
import org.schedoscope.test.test
import org.schedoscope.dsl.Field._



class NodesWithGeohashTest extends FlatSpec with Matchers {
  
  val stageNodes = new schedoscope.example.osm.stage.Nodes() with rows {
    set(
        v(longitude, 10.0232716), 
        v(latitude, 53.5282633))
    set(
        v(longitude, 10.0243161), 
        v(latitude, 53.5297589))     
  }
  
  "NodesWithGeoHash" should "load correctly from stage.nodes" in {
    new NodesWithGeohash() with test {
      basedOn(stageNodes)
      withConfiguration("input_path", stageNodes.fullPath)
      then
      numRows shouldBe 2
      row(v(geohash) shouldBe "t1y140djfcq0")
      row(v(geohash) shouldBe "t1y140g5vgcn")
    }
    
  } 
}