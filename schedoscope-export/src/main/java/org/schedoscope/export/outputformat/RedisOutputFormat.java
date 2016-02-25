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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.schedoscope.export.JdbcExportMapper;
import org.schedoscope.export.utils.RedisMRJedisFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisOutputFormat<K extends RedisWritable, V> extends OutputFormat<K, V> {

	public static final String REDIS_CONNECT_STRING = "redis.export.server.host";
	public static final String REDIS_PIPELINE_MODE = "redis.export.pipeline.mode";

	private static final Log LOG = LogFactory.getLog(JdbcExportMapper.class);

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
