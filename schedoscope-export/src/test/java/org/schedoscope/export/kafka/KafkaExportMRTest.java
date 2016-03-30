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

package org.schedoscope.export.kafka;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.kafka.avro.HCatToAvroRecordConverter;
import org.schedoscope.export.kafka.outputformat.CleanupPolicy;
import org.schedoscope.export.kafka.outputformat.CompressionCodec;
import org.schedoscope.export.kafka.outputformat.KafkaOutputFormat;
import org.schedoscope.export.kafka.outputformat.ProducerType;
import org.schedoscope.export.testsupport.EmbeddedKafkaCluster;

public class KafkaExportMRTest extends HiveUnitBaseTest {

	protected EmbeddedKafkaCluster kafka;

	protected TestingServer zkServer;

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		zkServer = new TestingServer(2182);
		zkServer.start();
		Thread.sleep(1000);
		startKafkaServer();
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
		stopKafkaServer();
		zkServer.close();
	}

	@Test
	public void testKafkaMapExport() throws Exception {

		Job job = Job.getInstance(conf);

		Schema schema = HCatToAvroRecordConverter.convertSchema(hcatInputSchema, "MyTable");
		AvroJob.setMapOutputValueSchema(job, schema);
		KafkaOutputFormat.setOutput(job.getConfiguration(), "localhost:9092", zkServer.getConnectString(),
				ProducerType.sync, CleanupPolicy.delete, "id", "test_map", 1, 1, CompressionCodec.gzip);

		job.setMapperClass(KafkaExportMapper.class);
		job.setReducerClass(KafkaExportReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(KafkaOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AvroValue.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvroValue.class);

		assertTrue(job.waitForCompletion(true));
	}

	private void startKafkaServer() throws Exception {

		ArrayList<Integer> ports = new ArrayList<Integer>();
		ports.add(9092);
		kafka = new EmbeddedKafkaCluster(zkServer.getConnectString(), new Properties(), ports);
		kafka.startup();
		Thread.sleep(2000);
	}

	private void stopKafkaServer() throws Exception {

		kafka.shutdown();
		Thread.sleep(1000);
	}
}
