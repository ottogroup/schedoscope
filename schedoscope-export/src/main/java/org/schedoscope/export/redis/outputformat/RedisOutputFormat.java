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

/**
 * The Redis output format is responsible to write data into Redis, initializes
 * the Redis Record Writer.
 *
 * @param <K>
 *            The key class,must be sub class of Rediswritable
 * @param <V>
 *            The value class.
 */
public class RedisOutputFormat<K extends RedisWritable, V> extends OutputFormat<K, V> {

	public static final String REDIS_EXPORT_SERVER_HOST = "redis.export.server.host";

	public static final String REDIS_EXPORT_SERVER_PORT = "redis.export.server.port";

	public static final String REDIS_EXPORT_SERVER_DB = "redis.export.server.db";

	public static final String REDIS_PIPELINE_MODE = "redis.export.pipeline.mode";

	public static final String REDIS_EXPORT_KEY_NAME = "redis.export.key.name";

	public static final String REDIS_EXPORT_VALUE_NAME = "redis.export.value.name";

	public static final String REDIS_EXPORT_VALUE_REPLACE = "redis.export.value.replace";

	public static final String REDIS_EXPORT_KEY_PREFIX = "redis.export.key.prefix";

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException {

		/*
		 * Jedis jedis =
		 * RedisMRJedisFactory.getJedisClient(context.getConfiguration());
		 * LOG.info("set up redis: " + jedis.ping()); jedis.close();
		 */
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {

		return (new NullOutputFormat<NullWritable, NullWritable>()).getOutputCommitter(context);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {

		Configuration conf = context.getConfiguration();

		Jedis jedis = RedisMRJedisFactory.getJedisClient(conf);

		boolean replace = conf.getBoolean(REDIS_EXPORT_VALUE_REPLACE, true);

		if (conf.getBoolean(REDIS_PIPELINE_MODE, false)) {
			Pipeline pipelinedJedis = jedis.pipelined();
			return new PipelinedRedisRecordWriter(pipelinedJedis, replace);
		} else {
			return new RedisRecordWriter(jedis, replace);
		}
	}

	/**
	 * Returns the optional key prefix to prepend to the Redis key.
	 *
	 * @param conf
	 *            The Hadoop configuration object.
	 * @return The prefix as string.
	 */
	public static String getExportKeyPrefix(Configuration conf) {

		String prefix = conf.get(REDIS_EXPORT_KEY_PREFIX, "");
		StringBuilder keyPrefixBuilder = new StringBuilder();
		if (!prefix.isEmpty()) {
			keyPrefixBuilder.append(prefix).append("_");
		}
		return keyPrefixBuilder.toString();
	}

	/**
	 * Initializes the RedisOutputFormat.
	 *
	 * @param conf
	 *            The Hadoop configuration object.
	 * @param redisHost
	 *            The Redis hostname
	 * @param redisPort
	 *            The Redis port
	 * @param redisDb
	 *            The Redis database.
	 * @param keyName
	 *            The name of the key field
	 * @param keyPrefix
	 *            The key prefix
	 * @param valueName
	 *            The name of the value field
	 * @param replace
	 *            A flag indicating if existing data should be replaced
	 * @param pipeline
	 *            A flag to use the Redis pipeline mode.
	 */
	public static void setOutput(Configuration conf, String redisHost, int redisPort, int redisDb, String keyName,
			String keyPrefix, String valueName, boolean replace, boolean pipeline) {

		conf.set(REDIS_EXPORT_SERVER_HOST, redisHost);
		conf.setInt(REDIS_EXPORT_SERVER_PORT, redisPort);
		conf.setInt(REDIS_EXPORT_SERVER_DB, redisDb);
		conf.set(REDIS_EXPORT_KEY_NAME, keyName);
		conf.set(REDIS_EXPORT_KEY_PREFIX, keyPrefix);
		conf.set(REDIS_EXPORT_VALUE_NAME, valueName);
		conf.setBoolean(REDIS_EXPORT_VALUE_REPLACE, replace);
		conf.setBoolean(REDIS_PIPELINE_MODE, pipeline);
	}

	public static void setOutput(Configuration conf, String redisHost, int redisPort, int redisDb, String keyName,
			String keyPrefix, boolean replace, boolean pipeline) {

		setOutput(conf, redisHost, redisPort, redisDb, keyName, keyPrefix, "", replace, pipeline);
	}

	/**
	 * A function to return the writable depending on the name of the value
	 * field.
	 *
	 * @param schema
	 *            The Hcatalog schema
	 * @param fieldName
	 *            The name of the field.
	 * @return A class used as writable
	 * @throws IOException
	 *             Thrown if an error occurs.
	 */
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
		default:
			break;
		}
		return RWClazz;
	}

	/**
	 * The Redis Record Writer is used to write data into Redis.
	 */
	public class RedisRecordWriter extends RecordWriter<K, V> {

		private Jedis jedis;

		boolean replace;

		/**
		 * The constructor to initialize the record writer.
		 *
		 * @param jedis
		 *            The redis client.
		 * @param replace
		 *            A flag to enable replace mode.
		 */
		public RedisRecordWriter(Jedis jedis, boolean replace) {

			this.jedis = jedis;
			this.replace = replace;
		}

		@Override
		public void write(K key, V value) {

			key.write(jedis, replace);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {

			jedis.close();
		}
	}

	/**
	 * A piplined version of the Redis Record Writer, uses pipeline mode to
	 * write data into Redis.
	 */
	public class PipelinedRedisRecordWriter extends RecordWriter<K, V> {

		private Pipeline jedis;

		private boolean replace;

		/**
		 * The constructor to initialize the pipelined writer.
		 *
		 * @param jedis
		 *            The pipelined Redis client.
		 * @param replace
		 *            A flag to enable replace mode.
		 */
		public PipelinedRedisRecordWriter(Pipeline jedis, boolean replace) {

			this.jedis = jedis;
			this.replace = replace;
		}

		@Override
		public void write(K key, V value) {

			key.write(jedis, replace);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {

			jedis.sync();
			jedis.close();
		}
	}
}
