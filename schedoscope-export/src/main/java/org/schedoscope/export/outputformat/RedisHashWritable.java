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
package org.schedoscope.export.outputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisHashWritable implements RedisWritable, Writable {

	private Text key;

	private MapWritable value;

	private BooleanWritable append;

	public RedisHashWritable() {

		key = new Text();
		value = new MapWritable();
		append = new BooleanWritable(false);
	}

	public RedisHashWritable(String key, Map<String, String> value, Boolean append) {

		this.key = new Text(String.valueOf(key));
		this.value = toMapWritable(value);
		this.append = new BooleanWritable(append);
	}

	public RedisHashWritable(String key, Map<String, String> value) {
		this(key, value, false);
	}

	public RedisHashWritable(Text key, MapWritable value, BooleanWritable append) {

		this.key = key;
		this.value = value;
		this.append = append;
	}

	public RedisHashWritable(Text key, MapWritable value) {
		this(key, value, new BooleanWritable(false));
	}

	@Override
	public void write(Jedis jedis) {
		if (!append.get()) {
			jedis.del(key.toString());
		}
		jedis.hmset(key.toString(), fromMapWritable(value));
	}

	@Override
	public void write(Pipeline jedis) {
		if (!append.get()) {
			jedis.del(key.toString());
		}
		jedis.hmset(key.toString(), fromMapWritable(value));
	}

	@Override
	public void readFields(Jedis jedis, String key) {
		this.value = toMapWritable(jedis.hgetAll(key));
		this.key = new Text(key);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
		append.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
		append.readFields(in);
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
