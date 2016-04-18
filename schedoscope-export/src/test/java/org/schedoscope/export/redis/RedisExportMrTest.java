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

package org.schedoscope.export.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rarefiedredis.redis.adapter.jedis.JedisAdapter;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.redis.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.RedisMRJedisFactory;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.*", "org.apache.*", "com.*", "org.mortbay.*",
		"org.xml.*", "org.w3c.*" })
@PrepareForTest(RedisMRJedisFactory.class)
public class RedisExportMrTest extends HiveUnitBaseTest {

	JedisAdapter jedisAdapter;

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		jedisAdapter = new JedisAdapter();
		PowerMockito.mockStatic(RedisMRJedisFactory.class);
		when(RedisMRJedisFactory.getJedisClient(any(Configuration.class)))
				.thenReturn(jedisAdapter);
	}

	@Test
	public void testRedisStringExport() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt",
				"src/test/resources/test_map.hql", "test_map");

		final String KEY = "id";
		final String VALUE = "created_at";

		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "string_export");
		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME, VALUE);
		// conf.setBoolean(RedisOutputFormat.REDIS_PIPELINE_MODE, true);

		Class<?> OutputClazz = RedisOutputFormat.getRedisWritableClazz(
				hcatInputSchema, VALUE);

		Job job = Job.getInstance(conf);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputClazz);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutputClazz);

		assertTrue(job.waitForCompletion(true));

		assertEquals(
				"2016-02-09T12:21:24.581+01:00",
				jedisAdapter
						.get("string_export_0000434c-aa04-449d-b6d5-319da5d94064"));
		assertEquals(
				"2016-02-09T12:21:24.581+01:00",
				jedisAdapter
						.get("string_export_00017475-db44-495f-a357-97cd277e9d5b"));
	}

	@Test
	public void testRedisMapExport() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt",
				"src/test/resources/test_map.hql", "test_map");

		final String KEY = "id";
		final String VALUE = "type";

		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "map_export");
		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME, VALUE);

		Class<?> OutputClazz = RedisOutputFormat.getRedisWritableClazz(
				hcatInputSchema, VALUE);

		Job job = Job.getInstance(conf);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputClazz);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutputClazz);

		assertTrue(job.waitForCompletion(true));
		assertEquals("3", jedisAdapter.hget(
				"map_export_00017475-db44-495f-a357-97cd277e9d5b", "a55"));
		assertEquals("2", jedisAdapter.hget(
				"map_export_0000e5da-0c7f-43d4-ba63-c7cd74eca7f6", "a621"));
	}

	@Test
	public void testRedisListExport() throws Exception {

		setUpHiveServer("src/test/resources/test_array_data.txt",
				"src/test/resources/test_array.hql", "test_array");

		final String KEY = "id";
		final String VALUE = "type";

		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "list_export");
		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME, VALUE);

		Class<?> OutputClazz = RedisOutputFormat.getRedisWritableClazz(
				hcatInputSchema, VALUE);

		Job job = Job.getInstance(conf);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputClazz);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutputClazz);

		assertTrue(job.waitForCompletion(true));
		assertEquals(
				"value2",
				jedisAdapter
						.lpop("list_export_1438843758818ab9c238f-c715-4dcc-824f-26346233ccd5-2015-08-20-000036"));
	}

	@Test
	public void testRedisStructExport() throws Exception {

		setUpHiveServer("src/test/resources/test_struct_data.txt",
				"src/test/resources/test_struct.hql", "test_struct");

		final String KEY = "id";
		final String VALUE = "type";

		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "struct_export");
		conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME, VALUE);

		Class<?> OutputClazz = RedisOutputFormat.getRedisWritableClazz(
				hcatInputSchema, VALUE);

		Job job = Job.getInstance(conf);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputClazz);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutputClazz);

		assertTrue(job.waitForCompletion(true));
		assertEquals(
				"value1",
				jedisAdapter
						.hget("struct_export_1438843758818ab9c238f-c715-4dcc-824f-26346233ccd5-2015-08-20-000030",
								"field1"));
	}
}
