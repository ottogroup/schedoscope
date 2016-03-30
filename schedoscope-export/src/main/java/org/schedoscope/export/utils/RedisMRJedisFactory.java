/**
 * Copyright 2016 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.utils;

import org.apache.hadoop.conf.Configuration;
import org.schedoscope.export.redis.outputformat.RedisOutputFormat;

import redis.clients.jedis.Jedis;

/**
 * Class provides a single static function to
 * return a configured Redis client.
 */
public class RedisMRJedisFactory {

    private static volatile Jedis jedis = null;

    /**
     * Returns a configured Redis client.
     *
     * @param conf The Hadoop configuration object.
     * @return The configured Redis client.
     */
    public static Jedis getJedisClient(Configuration conf) {

        if (jedis == null) {
            jedis = new Jedis(conf.get(RedisOutputFormat.REDIS_EXPORT_SERVER_HOST, "localhost"),
                            conf.getInt(RedisOutputFormat.REDIS_EXPORT_SERVER_PORT, 6379),
                            1800);
        }
        int redisDb = conf.getInt(RedisOutputFormat.REDIS_EXPORT_SERVER_DB, 0);
        jedis.select(redisDb);
        return jedis;
    }
}
