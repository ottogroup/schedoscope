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

package org.schedoscope.export.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputFormat;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputWritable;
import org.schedoscope.export.jdbc.outputschema.Schema;
import org.schedoscope.export.jdbc.outputschema.SchemaFactory;
import org.schedoscope.export.jdbc.outputschema.SchemaUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JdbcExportJobMRFullTest extends HiveUnitBaseTest {

	private static final String JDBC_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";

	private static final String CREATE_CONNECTION_STRING = "jdbc:derby:memory:TestingDB;create=true";

	private static final String CONNECTION_STRING = "jdbc:derby:memory:TestingDB";

	private static final int NUM_PARTITIONS = 2;

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();

		Class.forName(JDBC_DRIVER_NAME);
		DriverManager.getConnection(CREATE_CONNECTION_STRING);
	}

	@Test
	public void testRunMrJobMap() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt",
				"src/test/resources/test_map.hql", "test_map");

		Job job = Job.getInstance(conf);

		job.setMapperClass(JdbcExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(NUM_PARTITIONS);

		Schema outputSchema = SchemaFactory.getSchema(CONNECTION_STRING,
				job.getConfiguration());

		String[] columnNames = SchemaUtils.getColumnNamesFromHcatSchema(
				hcatInputSchema, outputSchema);
		String[] columnTypes = SchemaUtils.getColumnTypesFromHcatSchema(
				hcatInputSchema, outputSchema);

		JdbcOutputFormat.setOutput(job.getConfiguration(), CONNECTION_STRING,
				null, null, "testing", null, NUM_PARTITIONS, 10000, null, null,
				columnNames, columnTypes);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(JdbcOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JdbcOutputWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(JdbcOutputWritable.class);

		assertTrue(job.waitForCompletion(true));
		JdbcOutputFormat.finalizeOutput(job.getConfiguration());

		Connection conn = outputSchema.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM testing");
		while (rs.next()) {
			assertEquals(10, rs.getInt(1));
		}

		ObjectMapper objMapper = new ObjectMapper();
		Statement stmt2 = conn.createStatement();
		ResultSet rs2 = stmt2
				.executeQuery("SELECT type FROM testing ORDER BY id");
		if (rs2.next()) {
			String data = rs2.getString(1);
			assertEquals(2, objMapper.readTree(data).get("a821").asInt());
		}
	}

	@Test
	public void testRunMrJobArray() throws Exception {

		setUpHiveServer("src/test/resources/test_array_data.txt",
				"src/test/resources/test_array.hql", "test_array");

		Job job = Job.getInstance(conf);

		job.setMapperClass(JdbcExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(NUM_PARTITIONS);

		Schema outputSchema = SchemaFactory.getSchema(CONNECTION_STRING,
				job.getConfiguration());

		String[] columnNames = SchemaUtils.getColumnNamesFromHcatSchema(
				hcatInputSchema, outputSchema);
		String[] columnTypes = SchemaUtils.getColumnTypesFromHcatSchema(
				hcatInputSchema, outputSchema);

		JdbcOutputFormat.setOutput(job.getConfiguration(), CONNECTION_STRING,
				null, null, "testing", null, NUM_PARTITIONS, 10000, null, null,
				columnNames, columnTypes);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(JdbcOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JdbcOutputWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(JdbcOutputWritable.class);

		assertTrue(job.waitForCompletion(true));
		JdbcOutputFormat.finalizeOutput(job.getConfiguration());

		Connection conn = outputSchema.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM testing");
		while (rs.next()) {
			assertEquals(10, rs.getInt(1));
		}

		ObjectMapper objMapper = new ObjectMapper();
		Statement stmt2 = conn.createStatement();
		ResultSet rs2 = stmt2
				.executeQuery("SELECT type FROM testing ORDER BY id");
		while (rs2.next()) {
			String data = rs2.getString(1);
			assertEquals("value1", objMapper.readTree(data).get(0).asText());
			assertEquals("value2", objMapper.readTree(data).get(1).asText());
		}
	}

	@Test
	public void testRunMrJobAnonymized() throws Exception {

		setUpHiveServer("src/test/resources/test_array_data.txt",
				"src/test/resources/test_array.hql", "test_array");

		conf.setStrings(BaseExportJob.EXPORT_ANON_FIELDS, new String[] {"month_id"});

		Job job = Job.getInstance(conf);

		job.setMapperClass(JdbcExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(NUM_PARTITIONS);

		Schema outputSchema = SchemaFactory.getSchema(CONNECTION_STRING,
				job.getConfiguration());

		String[] columnNames = SchemaUtils.getColumnNamesFromHcatSchema(
				hcatInputSchema, outputSchema);
		String[] columnTypes = SchemaUtils.getColumnTypesFromHcatSchema(
				hcatInputSchema, outputSchema);

		JdbcOutputFormat.setOutput(job.getConfiguration(), CONNECTION_STRING,
				null, null, "testing", null, NUM_PARTITIONS, 10000, null, null,
				columnNames, columnTypes);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(JdbcOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JdbcOutputWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(JdbcOutputWritable.class);

		assertTrue(job.waitForCompletion(true));
		JdbcOutputFormat.finalizeOutput(job.getConfiguration());

		Connection conn = outputSchema.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM testing");
		while (rs.next()) {
			assertEquals(10, rs.getInt(1));
		}

		Statement stmt2 = conn.createStatement();
		ResultSet rs2 = stmt2
				.executeQuery("SELECT month_id FROM testing ORDER BY id");
		while (rs2.next()) {
			assertEquals("5891c145610d1517d7c844fe0a2aac39", rs2.getString(1));
		}
	}

	@Test
	public void testRunMrJobStruct() throws Exception {

		setUpHiveServer("src/test/resources/test_struct_data.txt",
				"src/test/resources/test_struct.hql", "test_struct");

		Job job = Job.getInstance(conf);

		job.setMapperClass(JdbcExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(NUM_PARTITIONS);

		Schema outputSchema = SchemaFactory.getSchema(CONNECTION_STRING,
				job.getConfiguration());

		String[] columnNames = SchemaUtils.getColumnNamesFromHcatSchema(
				hcatInputSchema, outputSchema);
		String[] columnTypes = SchemaUtils.getColumnTypesFromHcatSchema(
				hcatInputSchema, outputSchema);

		JdbcOutputFormat.setOutput(job.getConfiguration(), CONNECTION_STRING,
				null, null, "testing", null, NUM_PARTITIONS, 10000, null, null,
				columnNames, columnTypes);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(JdbcOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JdbcOutputWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(JdbcOutputWritable.class);

		assertTrue(job.waitForCompletion(true));
		JdbcOutputFormat.finalizeOutput(job.getConfiguration());

		Connection conn = outputSchema.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM testing");
		while (rs.next()) {
			assertEquals(10, rs.getInt(1));
		}

		ObjectMapper objMapper = new ObjectMapper();
		Statement stmt2 = conn.createStatement();
		ResultSet rs2 = stmt2
				.executeQuery("SELECT type FROM testing ORDER BY id");
		while (rs2.next()) {
			String data = rs2.getString(1);
			assertEquals("value1", objMapper.readTree(data).get("field1")
					.asText());
			assertEquals("value2", objMapper.readTree(data).get("field2")
					.asText());
		}
	}
}
