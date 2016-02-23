package org.schedoscope.export.outputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisListWritable implements RedisWritable, Writable {

	private Text key;

	private ArrayWritable value;

	public RedisListWritable() {
		key = new Text();
		value = new ArrayWritable(Text.class);
	}

	public RedisListWritable(String key, List<String> value) {
		this.key = new Text(String.valueOf(key));
		this.value = toArrayWritable(value);
	}

	public RedisListWritable(Text key, ArrayWritable value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(Pipeline jedis) {
		for (String v : fromArrayWritable(value)) {
			jedis.lpush(key.toString(), v);
		}
	}

	@Override
	public void readFields(Jedis jedis, String key) {
		this.value = toArrayWritable(jedis.lrange(key, 0, -1));
		this.key = new Text(key);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
	}

	ArrayWritable toArrayWritable(List<String> value) {

		Text[] ar = new Text[value.size()];
		for (int i = 0; i < value.size(); i++) {
			ar[i] = new Text(value.get(i));
		}
		ArrayWritable aw = new ArrayWritable(Text.class, ar);
		return aw;
	}

	List<String> fromArrayWritable(ArrayWritable value) {
		return Arrays.asList(value.toStrings());
	}
}
