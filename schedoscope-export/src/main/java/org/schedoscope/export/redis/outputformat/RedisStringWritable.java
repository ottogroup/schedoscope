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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable to store a string, provides functions to store data in Redis.
 */
public class RedisStringWritable implements RedisWritable, Writable {

    private Text key;

    private Text value;

    /**
     * Default constructor, initializes the internal writables.
     */
    public RedisStringWritable() {

        key = new Text();
        value = new Text();
    }

    /**
     * Constructor to initialize with a key and a list of elements.
     *
     * @param key   The key to use for Redis.
     * @param value The data to write into Redis.
     */
    public RedisStringWritable(String key, String value) {

        this.key = new Text(String.valueOf(key));
        this.value = new Text(String.valueOf(value));
    }

    /**
     * A constructor setting the internal writables.
     *
     * @param key   The Redis key
     * @param value The Redis value
     */
    public RedisStringWritable(Text key, Text value) {

        this.key = key;
        this.value = value;
    }

    @Override
    public void write(Jedis jedis, boolean replace) {

        jedis.set(key.toString(), value.toString());
    }

    @Override
    public void write(Pipeline jedis, boolean replace) {

        jedis.set(key.toString(), value.toString());
    }

    @Override
    public void write(DataOutput out) throws IOException {

        key.write(out);
        value.write(out);
    }

    @Override
    public void readFields(Jedis jedis, String key) {

        this.value = new Text(jedis.get(key));
        this.key = new Text(key);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        key.readFields(in);
        value.readFields(in);
    }
}
