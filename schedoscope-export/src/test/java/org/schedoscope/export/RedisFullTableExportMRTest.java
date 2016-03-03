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

import static org.junit.Assert.assertEquals;
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
import org.schedoscope.export.outputformat.RedisHashWritable;
import org.schedoscope.export.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.RedisMRJedisFactory;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.*", "org.apache.*", "com.*", "org.mortbay.*", "org.xml.*", "org.w3c.*" })
@PrepareForTest(RedisMRJedisFactory.class)
public class RedisFullTableExportMRTest extends HiveUnitBaseTest {

    JedisAdapter jedisAdapter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jedisAdapter = new JedisAdapter();
        PowerMockito.mockStatic(RedisMRJedisFactory.class);
        when(RedisMRJedisFactory.getJedisClient(any(Configuration.class))).thenReturn(jedisAdapter);
    }

    @Test
    public void testRedisFullExport1() throws Exception {

        setUpHiveServer("src/test/resources/ogm_event_features_data.txt", "src/test/resources/ogm_event_features.hql",
                "ogm_event_features");

        final String KEY = "visitor_id";
        conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "export1");
        conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);

        Job job = Job.getInstance(conf);

        job.setMapperClass(RedisFullTableExportMapper.class);
        job.setReducerClass(RedisExportReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(RedisOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RedisHashWritable.class);
        job.setOutputKeyClass(RedisHashWritable.class);
        job.setOutputValueClass(NullWritable.class);

        assertTrue(job.waitForCompletion(true));
        assertEquals("2016-02-09T12:21:24.581+01:00",
                jedisAdapter.hget("export1_0000e5da-0c7f-43d4-ba63-c7cd74eca7f6", "created_at"));
        assertEquals("{\"a817\":3,\"a91\":3,\"a942\":3,\"a239\":3,\"a751\":3,\"a674\":3}",
                jedisAdapter.hget("export1_000202f5-6f6a-47af-b7aa-6c9b371dc87c", "uri_path_hashed_count"));
    }

    @Test
    public void testRedisFullExport2() throws Exception {

        setUpHiveServer("src/test/resources/webtrends_event_data.txt", "src/test/resources/webtrends_event.hql",
                "webtrends_event");

        final String KEY = "id";
        conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "export2");
        conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);

        Job job = Job.getInstance(conf);

        job.setMapperClass(RedisFullTableExportMapper.class);
        job.setReducerClass(RedisExportReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(RedisOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RedisHashWritable.class);
        job.setOutputKeyClass(RedisHashWritable.class);
        job.setOutputValueClass(NullWritable.class);

        assertTrue(job.waitForCompletion(true));
        assertEquals("[\"product_listing_display\",\"search_result_display\"]", jedisAdapter
                .hget("export2_1438843758818ab9c238f-c715-4dcc-824f-26346233ccd5-2015-08-20-000036", "type"));
    }

    @Test
    public void testRedisFullExport3() throws Exception {

        setUpHiveServer("src/test/resources/webtrends_struct_data.txt", "src/test/resources/webtrends_struct.hql",
                "webtrends_struct");

        final String KEY = "id";
        conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_PREFIX, "export3");
        conf.set(RedisOutputFormat.REDIS_EXPORT_KEY_NAME, KEY);

        Job job = Job.getInstance(conf);

        job.setMapperClass(RedisFullTableExportMapper.class);
        job.setReducerClass(RedisExportReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(RedisOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RedisHashWritable.class);
        job.setOutputKeyClass(RedisHashWritable.class);
        job.setOutputValueClass(NullWritable.class);

        assertTrue(job.waitForCompletion(true));
    }
}
