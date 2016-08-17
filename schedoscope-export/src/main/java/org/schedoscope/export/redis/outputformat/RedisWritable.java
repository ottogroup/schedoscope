/**
 * Copyright 2016 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.redis.outputformat;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * The base class for all RedisWritables, takes care of writing data back to
 * redis KV store.
 */
public interface RedisWritable {

    /**
     * Write data back to redis using the regular client.
     *
     * @param jedis
     *            The Redis client.
     * @param replace
     *            Flag to toggle replace mode.
     */
    public void write(Jedis jedis, boolean replace);

    /**
     * Write data back to redis using the pipelined client.
     *
     * @param jedis
     *            The Redis client.
     * @param replace
     *            Flag to toggle replace mode.
     */
    public void write(Pipeline jedis, boolean replace);

    /**
     * Read data from Redis using the regular client.
     *
     * @param jedis
     *            The Redis client
     * @param key
     *            The key to use for lookups.
     */
    public void readFields(Jedis jedis, String key);
}
