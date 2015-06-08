package schedoscope.example.osm.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ch.hsr.geohash.GeoHash;

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
