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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisOutputFormat<K extends RedisWritable, V> extends OutputFormat<K, V> {

	private static final String REDIS_CONNECT_STRING = "redis.export.server.host";
	private static final Log LOG = LogFactory.getLog(JdbcExportMapper.class);

	Jedis jedis;

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException {

		Jedis jedis = new Jedis(context.getConfiguration().get(REDIS_CONNECT_STRING, "localhost"));
		LOG.info("set up redis: " + jedis.ping());
		jedis.close();

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
		return (new NullOutputFormat<NullWritable, NullWritable>()).getOutputCommitter(context);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {

		Configuration conf = context.getConfiguration();

		Pipeline jedis = new Jedis(conf.get(REDIS_CONNECT_STRING, "localhost")).pipelined();
		return new RedisRecordWriter(jedis);
	}

	public class RedisRecordWriter extends RecordWriter<K, V> {

		private Pipeline jedis;

		public RedisRecordWriter(Pipeline jedis) {
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
