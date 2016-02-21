package org.schedoscope.export;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.schedoscope.export.outputformat.RedisHashWritable;

public class RedisExportReducer extends Reducer<Text, NullWritable, RedisHashWritable, NullWritable> {
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

		Map<String, String> mapData = new HashMap<String, String>();
		mapData.put("nase", "kunde");
		mapData.put("horst", "kunde");
		RedisHashWritable redis = new RedisHashWritable("key123", mapData);
		context.write(redis, NullWritable.get());
	}
}
