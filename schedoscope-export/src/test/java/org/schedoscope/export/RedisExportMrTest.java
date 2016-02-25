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
package org.schedoscope.export;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rarefiedredis.redis.adapter.jedis.JedisAdapter;
import org.schedoscope.export.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.RedisMRJedisFactory;
import org.schedoscope.export.utils.RedisMRUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.*", "org.apache.*", "com.*", "org.mortbay.*", "org.xml.*", "org.w3c.*"})
@PrepareForTest(RedisMRJedisFactory.class)
public class RedisExportMrTest extends HiveUnitBaseTest {

	@Before
	public void setUp() throws Exception {
		super.setUp();
		JedisAdapter jedisAdapter = new JedisAdapter();
		PowerMockito.mockStatic(RedisMRJedisFactory.class);
		when(RedisMRJedisFactory.getJedisClient(any(Configuration.class))).thenReturn(jedisAdapter);
	}

	@Test
	public void testRedisStringExport() throws Exception {

		setUpHiveServer("src/test/resources/ogm_event_features_data.txt",
				"src/test/resources/ogm_event_features.hql",
				"ogm_event_features");

		final String KEY = "visitor_id";
		final String VALUE = "created_at";

		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_PREFIX, "string_export");
		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisExportMapper.REDIS_EXPORT_VALUE_NAME, VALUE);
		// conf.setBoolean(RedisOutputFormat.REDIS_PIPELINE_MODE, true);

		Job job = Job.getInstance(conf);

		Class<?> klass  = RedisMRUtils.getRedisWritableKlass(hcatInputSchema, VALUE);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(RedisExportReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(klass);
		job.setOutputKeyClass(klass);
		job.setOutputValueClass(NullWritable.class);

		assertTrue(job.waitForCompletion(true));
	}

	@Test
	public void testRedisMapExport() throws Exception {

		setUpHiveServer("src/test/resources/ogm_event_features_data.txt",
				"src/test/resources/ogm_event_features.hql",
				"ogm_event_features");

		final String KEY = "visitor_id";
		final String VALUE = "uri_path_hashed_count";

		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_PREFIX, "map_export");
		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisExportMapper.REDIS_EXPORT_VALUE_NAME, VALUE);

		Job job = Job.getInstance(conf);

		Class<?> OutputKlass  = RedisMRUtils.getRedisWritableKlass(hcatInputSchema, VALUE);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(RedisExportReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputKlass);
		job.setOutputKeyClass(OutputKlass);
		job.setOutputValueClass(NullWritable.class);

		assertTrue(job.waitForCompletion(true));
	}

	@Test
	public void testRedisListExport() throws Exception {

		setUpHiveServer("src/test/resources/webtrends_event_data.txt",
				"src/test/resources/webtrends_event.hql",
				"webtrends_event");

		final String KEY = "id";
		final String VALUE = "type";

		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_PREFIX, "list_export");
		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisExportMapper.REDIS_EXPORT_VALUE_NAME, VALUE);

		Job job = Job.getInstance(conf);

		Class<?> OutputKlass  = RedisMRUtils.getRedisWritableKlass(hcatInputSchema, VALUE);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(RedisExportReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputKlass);
		job.setOutputKeyClass(OutputKlass);
		job.setOutputValueClass(NullWritable.class);

		assertTrue(job.waitForCompletion(true));
	}
}
