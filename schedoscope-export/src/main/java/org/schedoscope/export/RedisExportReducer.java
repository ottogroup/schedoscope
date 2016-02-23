package org.schedoscope.export;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.schedoscope.export.outputformat.RedisWritable;

public class RedisExportReducer extends Reducer<Text, RedisWritable, RedisWritable, NullWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<RedisWritable> values, Context context) throws IOException, InterruptedException {

		for (RedisWritable w : values) {
			context.write(w, NullWritable.get());
		}
	}
}
