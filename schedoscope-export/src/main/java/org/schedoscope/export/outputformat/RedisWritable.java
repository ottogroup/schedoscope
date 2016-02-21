package org.schedoscope.export.outputformat;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public interface RedisWritable {

	public void write(Pipeline jedis);

	public void readFields(Jedis jedis, String key);
}
