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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A writable to store maps, provides a function to store the data in Redis.
 */
public class RedisHashWritable implements RedisWritable, Writable {

    private Text key;

    private MapWritable value;

    /**
     * Default constructor, initializes the internal writables.
     */
    public RedisHashWritable() {

        key = new Text();
        value = new MapWritable();
    }

    /**
     * Constructor to initialize with a key and a list of elements.
     *
     * @param key
     *            The key to use for Redis.
     * @param value
     *            The data to write into Redis.
     */
    public RedisHashWritable(String key, Map<String, String> value) {

        this.key = new Text(String.valueOf(key));
        this.value = toMapWritable(value);
    }

    /**
     * A constructor setting the internal writables.
     *
     * @param key
     *            The Redis key
     * @param value
     *            The Redis value
     */
    public RedisHashWritable(Text key, MapWritable value) {

        this.key = key;
        this.value = value;
    }

    @Override
    public void write(Jedis jedis, boolean replace) {

        if (replace) {
            jedis.del(key.toString());
        }
        jedis.hmset(key.toString(), fromMapWritable(value));
    }

    @Override
    public void write(Pipeline jedis, boolean replace) {

        if (replace) {
            jedis.del(key.toString());
        }
        jedis.hmset(key.toString(), fromMapWritable(value));
    }

    @Override
    public void write(DataOutput out) throws IOException {

        key.write(out);
        value.write(out);
    }

    @Override
    public void readFields(Jedis jedis, String key) {

        this.value = toMapWritable(jedis.hgetAll(key));
        this.key = new Text(key);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        key.readFields(in);
        value.readFields(in);
    }

    private MapWritable toMapWritable(Map<String, String> value) {

        MapWritable mr = new MapWritable();
        for (Entry<String, String> e : value.entrySet()) {
            mr.put(new Text(e.getKey()), new Text(String.valueOf(e.getValue())));
        }
        return mr;
    }

    private Map<String, String> fromMapWritable(MapWritable value) {

        Map<String, String> mapValue = new HashMap<String, String>();
        for (Entry<Writable, Writable> e : value.entrySet()) {
            mapValue.put(e.getKey().toString(), e.getValue().toString());
        }
        return mapValue;
    }
}
