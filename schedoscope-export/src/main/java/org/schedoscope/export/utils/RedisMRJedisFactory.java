package org.schedoscope.export.utils;

import org.apache.hadoop.conf.Configuration;
import org.schedoscope.export.outputformat.RedisOutputFormat;

import redis.clients.jedis.Jedis;

/**
 * Class provides a single static function to
 * return a configured Redis client.
 */
public class RedisMRJedisFactory {

    private static Jedis jedis = null;

    /**
     * Returns a configured Redis client.
     *
     * @param conf The Hadoop configuration object.
     * @return The configured Redis client.
     */
    public static Jedis getJedisClient(Configuration conf) {

        if (jedis == null) {
            jedis = new Jedis(conf.get(RedisOutputFormat.REDIS_CONNECT_STRING, "localhost"));
        }
        return jedis;
    }
}
