package schedoscope.example.mapreduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.NullWritable
import java.io.IOException
import java.lang.Double
import org.apache.commons.lang3.StringUtils
import ch.hsr.geohash.GeoHash
import org.apache.hadoop.mapreduce.MapContext
import org.apache.hadoop.io.LongWritable


class GeohashMapper extends Mapper[LongWritable, Text, LongWritable, Text] {  
  
  val hashPrecision = 12
  
  // FIXME: this doesn't work, because this method doesn't overwrite super.map - why??
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def map(key: LongWritable, value: Text, context: Context) : Unit = {
    val rec = key.toString.split("\t")
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(Double.valueOf(rec(4)), Double.valueOf(rec(5)), hashPrecision)
    context.write(key, new Text( (rec ++ geohash).mkString("\t") ))
  }
  
}