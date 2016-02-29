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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.utils.RedisMRJedisFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisOutputFormat<K extends RedisWritable, V> extends OutputFormat<K, V> {

	public static final String REDIS_CONNECT_STRING = "redis.export.server.host";

	public static final String REDIS_PIPELINE_MODE = "redis.export.pipeline.mode";

	public static final String REDIS_EXPORT_KEY_NAME = "redis.export.key.name";

	public static final String REDIS_EXPORT_VALUE_NAME = "redis.export.value.name";

	public static final String REDIS_EXPORT_DATA_APPEND = "redis.export.data.append";

	public static final String REDIS_EXPORT_KEY_PREFIX = "redis.export.key.prefix";

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException {

/*		Jedis jedis = RedisMRJedisFactory.getJedisClient(context.getConfiguration());
		LOG.info("set up redis: " + jedis.ping());
		jedis.close();*/
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
		return (new NullOutputFormat<NullWritable, NullWritable>()).getOutputCommitter(context);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {

		Configuration conf = context.getConfiguration();

		Jedis jedis = RedisMRJedisFactory.getJedisClient(conf);

		if (conf.getBoolean(REDIS_PIPELINE_MODE, false)) {
			Pipeline pipelinedJedis = jedis.pipelined();
			return new PipelinedRedisRecordWriter(pipelinedJedis);
		} else {
			return new RedisRecordWriter(jedis);
		}
	}

	public static void checkKeyType(HCatSchema schema, String fieldName) throws IOException {

		HCatFieldSchema keyType = schema.get(fieldName);
		HCatFieldSchema.Category category = keyType.getCategory();

		if (category != HCatFieldSchema.Category.PRIMITIVE) {
			throw new IllegalArgumentException("key must be primitive type");
		}
	}

	public static void checkValueType(HCatSchema schema, String fieldName) throws IOException {

		HCatFieldSchema valueType = schema.get(fieldName);
		HCatFieldSchema.Category valueCat = valueType.getCategory();

		if ((valueCat != HCatFieldSchema.Category.PRIMITIVE) && (valueCat != HCatFieldSchema.Category.MAP) && (valueCat != HCatFieldSchema.Category.ARRAY)) {
			throw new IllegalArgumentException("value must be one of primitive, list or map type");
		}

		if (valueType.getCategory() == HCatFieldSchema.Category.MAP) {
			if (valueType.getMapValueSchema().get(0).getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
				throw new IllegalArgumentException("map value type must be a primitive type");
			}
		}

		if (valueType.getCategory() == HCatFieldSchema.Category.ARRAY) {
			if (valueType.getArrayElementSchema().get(0).getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
				throw new IllegalArgumentException("array element type must be a primitive type");
			}
		}
	}

	public static Boolean getAppend(Configuration conf) {
		return conf.getBoolean(REDIS_EXPORT_DATA_APPEND, false);
	}

	public static String getExportKeyPrefix(Configuration conf) {

		String prefix = conf.get(REDIS_EXPORT_KEY_PREFIX, "");
		StringBuilder keyPrefixBuilder = new StringBuilder();
		if (!prefix.isEmpty()) {
			keyPrefixBuilder.append(prefix).append("_");
		}
		return keyPrefixBuilder.toString();
	}

	public static void setOutput(Configuration conf, String redisHost,
			String keyName, String keyPrefix, String valueName,
			boolean append, boolean pipeline) {
		conf.set(REDIS_CONNECT_STRING, redisHost);
		conf.set(REDIS_EXPORT_KEY_NAME, keyName);
		conf.set(REDIS_EXPORT_KEY_PREFIX, keyPrefix);
		conf.set(REDIS_EXPORT_VALUE_NAME, valueName);
		conf.setBoolean(REDIS_EXPORT_DATA_APPEND, append);
		conf.setBoolean(REDIS_PIPELINE_MODE, pipeline);
	}

	public static void setOutput(Configuration conf, String redisHost,
			String keyName, String keyPrefix, boolean append, boolean pipeline) {
		setOutput(conf, redisHost, keyName, keyPrefix, "", append, pipeline);
	}

	public static Class<?> getRedisWritableClazz(HCatSchema schema, String fieldName) throws IOException {
		HCatFieldSchema.Category category = schema.get(fieldName).getCategory();

		Class<?> RWClazz = null;

		switch (category) {
		case PRIMITIVE:
			RWClazz = RedisStringWritable.class;
			break;
		case MAP:
			RWClazz = RedisHashWritable.class;
			break;
		case ARRAY:
			RWClazz = RedisListWritable.class;
			break;
		case STRUCT:
			RWClazz = RedisHashWritable.class;
			break;
		}
		return RWClazz;
	}

	public class RedisRecordWriter extends RecordWriter<K, V> {

		private Jedis jedis;

		public RedisRecordWriter(Jedis jedis) {
			this.jedis = jedis;
		}

		@Override
		public void write(K key, V value) {
			key.write(jedis);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {
			jedis.close();
		}
	}

	public class PipelinedRedisRecordWriter extends RecordWriter<K, V> {

		private Pipeline jedis;

		public PipelinedRedisRecordWriter(Pipeline jedis) {
			this.jedis = jedis;
		}

		@Override
		public void write(K key, V value) {
			key.write(jedis);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {
			jedis.sync();
			jedis.close();
		}
	}
}
