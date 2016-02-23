package org.schedoscope.export.outputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisStringWritable implements RedisWritable, Writable {

	private Text key;

	private Text value;

	public RedisStringWritable() {
		key = new Text();
		value = new Text();
	}

	public RedisStringWritable(String key, String value) {
		this.key = new Text(String.valueOf(key));
		this.value = new Text(String.valueOf(value));
	}

	public RedisStringWritable(Text key, Text value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(Pipeline jedis) {
		jedis.set(key.toString(), value.toString());
	}

	@Override
	public void readFields(Jedis jedis, String key) {
		value = new Text(jedis.get(key));
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
}
