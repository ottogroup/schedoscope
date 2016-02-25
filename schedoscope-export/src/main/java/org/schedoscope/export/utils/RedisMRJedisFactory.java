package org.schedoscope.export.utils;

import org.apache.hadoop.conf.Configuration;
import org.schedoscope.export.outputformat.RedisOutputFormat;

import redis.clients.jedis.Jedis;

public class RedisMRJedisFactory {

	private static Jedis jedis = null;

	public static Jedis getJedisClient(Configuration conf) {

		if (jedis == null) {
			jedis = new Jedis(conf.get(RedisOutputFormat.REDIS_CONNECT_STRING, "localhost"));
		}
		return jedis;
	}
}
