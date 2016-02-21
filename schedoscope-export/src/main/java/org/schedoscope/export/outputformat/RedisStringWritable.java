package org.schedoscope.export.outputformat;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisStringWritable implements RedisWritable {

	private String key;

	private String value;

	public RedisStringWritable(String key, String value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(Pipeline jedis) {
		jedis.set(key, value);
	}

	@Override
	public void readFields(Jedis jedis, String key) {
		value = jedis.get(key);
		this.key = key;
	}

}
