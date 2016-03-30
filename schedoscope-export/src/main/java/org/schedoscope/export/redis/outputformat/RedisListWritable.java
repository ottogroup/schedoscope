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

package org.schedoscope.export.redis.outputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * A writable to store arrays, provides a function to write the data into Redis.
 */
public class RedisListWritable implements RedisWritable, Writable {

	private Text key;

	private ArrayWritable value;

	/**
	 * Default constructor, initializes the internal writables.
	 */
	public RedisListWritable() {

		key = new Text();
		value = new ArrayWritable(Text.class);
	}

	/**
	 * Constructor to initialize with a key and a list of elements.
	 * 
	 * @param key
	 *            The key to use for Redis.
	 * @param value
	 *            The data to write into Redis.
	 */
	public RedisListWritable(String key, List<String> value) {

		this.key = new Text(String.valueOf(key));
		this.value = toArrayWritable(value);
	}

	/**
	 * A constructor setting the internal writables.
	 *
	 * @param key
	 *            The Redis key
	 * @param value
	 *            The Redis value
	 */
	public RedisListWritable(Text key, ArrayWritable value) {

		this.key = key;
		this.value = value;
	}

	@Override
	public void write(Jedis jedis, boolean replace) {

		if (replace) {
			jedis.del(key.toString());
		}
		for (String v : fromArrayWritable(value)) {
			jedis.lpush(key.toString(), v);
		}
	}

	@Override
	public void write(Pipeline jedis, boolean replace) {

		if (replace) {
			jedis.del(key.toString());
		}
		for (String v : fromArrayWritable(value)) {
			jedis.lpush(key.toString(), v);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {

		key.write(out);
		value.write(out);
	}

	@Override
	public void readFields(Jedis jedis, String key) {

		this.value = toArrayWritable(jedis.lrange(key, 0, -1));
		this.key = new Text(key);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		key.readFields(in);
		value.readFields(in);
	}

	private ArrayWritable toArrayWritable(List<String> value) {

		Text[] ar = new Text[value.size()];
		for (int i = 0; i < value.size(); i++) {
			ar[i] = new Text(value.get(i));
		}
		ArrayWritable aw = new ArrayWritable(Text.class, ar);
		return aw;
	}

	private List<String> fromArrayWritable(ArrayWritable value) {

		return Arrays.asList(value.toStrings());
	}
}
