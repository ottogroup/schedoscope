package org.schedoscope.export.outputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisHashWritable implements RedisWritable, Writable {

	private String key;

	private Map<String, String> value;

	public RedisHashWritable() {}

	public RedisHashWritable(String key, Map<String, String> value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(Pipeline jedis) {
		jedis.hmset(key, value);
	}

	@Override
	public void readFields(Jedis jedis, String key) {
		value = jedis.hgetAll(key);
		this.key = key;
	}

	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void readFields(DataInput in) throws IOException {

	}

}
