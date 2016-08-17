/**
  * Copyright 2015 Otto (GmbH & Co KG)
  * <p/>
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * <p/>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p/>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package schedoscope.example.osm.mapreduce

import java.io.IOException

import ch.hsr.geohash.GeoHash.geoHashStringWithCharacterPrecision
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
  * GeohashMapper computes a geohash for each node based upon its longitude and
  * latitude.
  */
class GeohashMapper extends Mapper[LongWritable, Text, NullWritable, Text] {

  val hashPrecision = 12

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def map(key: LongWritable, input: Text, context: Mapper[LongWritable, Text, NullWritable, Text]#Context): Unit = {

    val inputRecord = input.toString().split("\t")

    val hash = geoHashStringWithCharacterPrecision(inputRecord(4).toDouble, inputRecord(5).toDouble, hashPrecision)

    val outputRecord = inputRecord :+ hash

    val output = new Text(outputRecord.mkString("\t"))

    context.write(NullWritable.get(), output)

  }
}
