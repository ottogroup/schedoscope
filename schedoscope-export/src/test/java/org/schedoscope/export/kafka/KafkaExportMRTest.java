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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Properties;

import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.kafka.avro.HCatToAvroSchemaConverter;
import org.schedoscope.export.kafka.options.CleanupPolicy;
import org.schedoscope.export.kafka.options.CompressionCodec;
import org.schedoscope.export.kafka.options.OutputEncoding;
import org.schedoscope.export.kafka.options.ProducerType;
import org.schedoscope.export.kafka.outputformat.KafkaOutputFormat;
import org.schedoscope.export.testsupport.EmbeddedKafkaCluster;
import org.schedoscope.export.testsupport.SimpleTestKafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class KafkaExportMRTest extends HiveUnitBaseTest {

	protected EmbeddedKafkaCluster kafka;

	protected TestingServer zkServer;

	protected SimpleTestKafkaConsumer kafkaConsumer;

	protected ZkClient zkClient;

	private static final String TEST_TOPIC = "test_tpoic";

	private static final int TEST_SIZE = 10;

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();

		zkServer = new TestingServer(2182);
		zkServer.start();
		Thread.sleep(1000);
		zkClient = new ZkClient(zkServer.getConnectString(), 30000, 30000,
				ZKStringSerializer$.MODULE$);

		startKafkaServer();
		kafkaConsumer = new SimpleTestKafkaConsumer(TEST_TOPIC,
				zkServer.getConnectString(), 10);
	}

	@Override
	@After
	public void tearDown() throws Exception {
		super.tearDown();
		kafkaConsumer.shutdown();
		stopKafkaServer();
		zkServer.close();
	}

	@Test
	public void testKafkaMapExportString() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt",
				"src/test/resources/test_map.hql", "test_map");

		Job job = Job.getInstance(conf);

		Schema schema = HCatToAvroSchemaConverter.convertSchema(
				hcatInputSchema, "MyTable");
		AvroJob.setMapOutputValueSchema(job, schema);
		KafkaOutputFormat.setOutput(job.getConfiguration(), "localhost:9092",
				zkServer.getConnectString(), ProducerType.sync,
				CleanupPolicy.delete, "id", TEST_TOPIC, 1, 1,
				CompressionCodec.gzip, OutputEncoding.string);

		job.setMapperClass(KafkaExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(KafkaOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AvroValue.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvroValue.class);

		assertTrue(job.waitForCompletion(true));

		ObjectMapper objMapper = new ObjectMapper();

		int counter = 0;
		for (byte[] message : kafkaConsumer) {
			counter++;
			String record = new String(message, Charsets.UTF_8);
			JsonNode data = objMapper.readTree(record);
			assertEquals("value1", data.get("created_by").asText());
		}

		assertEquals(TEST_SIZE, counter);
	}

	@Test
	public void testKafkaMapExportAvro() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt",
				"src/test/resources/test_map.hql", "test_map");

		Job job = Job.getInstance(conf);

		Schema schema = HCatToAvroSchemaConverter.convertSchema(
				hcatInputSchema, "MyTable");
		AvroJob.setMapOutputValueSchema(job, schema);
		KafkaOutputFormat.setOutput(job.getConfiguration(), "localhost:9092",
				zkServer.getConnectString(), ProducerType.sync,
				CleanupPolicy.delete, "id", TEST_TOPIC, 1, 1,
				CompressionCodec.gzip, OutputEncoding.avro);

		job.setMapperClass(KafkaExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(KafkaOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AvroValue.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvroValue.class);

		assertTrue(job.waitForCompletion(true));

		ObjectMapper objMapper = new ObjectMapper();

		SchemaRegistry registry = new MemorySchemaRegistry();
		registry.register(schema);
		FingerprintSerdeGeneric serde = new FingerprintSerdeGeneric(registry);

		int counter = 0;
		for (byte[] message : kafkaConsumer) {
			counter++;
			GenericRecord record = serde.fromBytes(message);
			JsonNode data = objMapper.readTree(record.toString());
			assertEquals("value1", data.get("created_by").asText());
		}
		assertEquals(TEST_SIZE, counter);
	}

	@Test
	public void testKafkaArrayStructExportAvro() throws Exception {

		setUpHiveServer("src/test/resources/test_arraystruct_data.txt",
				"src/test/resources/test_arraystruct.hql", "test_arraystruct");
		Job job = Job.getInstance(conf);

		Schema schema = HCatToAvroSchemaConverter.convertSchema(
				hcatInputSchema, "MyTable");
		AvroJob.setMapOutputValueSchema(job, schema);
		KafkaOutputFormat.setOutput(job.getConfiguration(), "localhost:9092",
				zkServer.getConnectString(), ProducerType.sync,
				CleanupPolicy.delete, "id", TEST_TOPIC, 1, 1,
				CompressionCodec.gzip, OutputEncoding.avro);

		job.setMapperClass(KafkaExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(KafkaOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AvroValue.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvroValue.class);

		assertTrue(job.waitForCompletion(true));

		ObjectMapper objMapper = new ObjectMapper();

		SchemaRegistry registry = new MemorySchemaRegistry();
		registry.register(schema);
		FingerprintSerdeGeneric serde = new FingerprintSerdeGeneric(registry);

		int counter = 0;
		for (byte[] message : kafkaConsumer) {
			counter++;
			GenericRecord record = serde.fromBytes(message);
			JsonNode data = objMapper.readTree(record.toString());
			assertEquals("value1", data.get("created_by").asText());
		}
		assertEquals(TEST_SIZE, counter);
	}

	private void startKafkaServer() throws Exception {

		ArrayList<Integer> ports = new ArrayList<Integer>();
		ports.add(9092);
		kafka = new EmbeddedKafkaCluster(zkServer.getConnectString(),
				new Properties(), ports);
		kafka.startup();
		Thread.sleep(2000);
	}

	private void stopKafkaServer() throws Exception {

		kafka.shutdown();
		Thread.sleep(1000);
	}
}
