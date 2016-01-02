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
package schedoscope.example.osm.mapreduce;

import ch.hsr.geohash.GeoHash;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * GeohashMapper computes a geohash for each node based upon its longitude and
 * latitude.
 */
public class GeohashMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	private int hashPrecision = 12;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] rec = value.toString().split("\t");
		String hash = GeoHash.geoHashStringWithCharacterPrecision(
				Double.valueOf(rec[4]), Double.valueOf(rec[5]), hashPrecision);
		String output = StringUtils.join(ArrayUtils.add(rec, hash), "\t");
		context.write(NullWritable.get(), new Text(output));
	}
}
