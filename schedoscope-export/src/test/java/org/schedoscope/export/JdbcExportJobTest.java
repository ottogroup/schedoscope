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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.outputformat.JdbcOutputFormat;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;

public class JdbcExportJobTest {

	private JdbcOutputFormat<JdbcOutputWritable, NullWritable> outputFormat = new JdbcOutputFormat<>();
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String CREATE_CONNECTIONSTRING = "jdbc:derby:memory:TestingDB;create=true";
	private static final String CONNECTIONSTRING = "jdbc:derby:memory:TestingDB";
	private static final String OUTPUTTABLE = "test";
	private static final int COMMITSIZE = 10;
	private static final String[] COLUMNNAMES = new String[] { "id", "val" };
	private static final String[] COLUMNTYPES = new String[] { "int",
			"varchar(100)" };

	private Configuration conf = new Configuration();

	@Before
	public void setUp() throws ClassNotFoundException, SQLException,
			IOException {
		Class.forName(DRIVER);
		DriverManager.getConnection(CREATE_CONNECTIONSTRING);

	}

	@Test
	public void testDatabaseExportWithMultipleWriters() throws IOException,
			SQLException, InterruptedException, ClassNotFoundException {

		JdbcOutputFormat.setOutput(conf, CONNECTIONSTRING, null, null,
				OUTPUTTABLE, null, 2, COMMITSIZE, null, null, COLUMNNAMES, COLUMNTYPES);
		Schema outputSchema = SchemaFactory.getSchema(conf);
		Connection connection = outputSchema.getConnection();
		TaskAttemptContext ctx1 = new TaskAttemptContextImpl(conf,
				new TaskAttemptID(new TaskID(new JobID("multipleReducers", 1),
						TaskType.REDUCE, 0), 1));
		TaskAttemptContext ctx2 = new TaskAttemptContextImpl(conf,
				new TaskAttemptID(new TaskID(new JobID("multipleReducers", 1),
						TaskType.REDUCE, 1), 1));
		RecordWriter<JdbcOutputWritable, NullWritable> rw1 = outputFormat
				.getRecordWriter(ctx1);
		RecordWriter<JdbcOutputWritable, NullWritable> rw2 = outputFormat
				.getRecordWriter(ctx2);

		List<Text> rows = new ArrayList<>();

		rows.add(new Text("1\ttest"));
		rows.add(new Text("2\ttest"));
		rows.add(new Text("3\ttest"));
		rows.add(new Text("4\ttest"));
		rows.add(new Text("5\ttest"));
		rows.add(new Text("6\ttest"));
		rows.add(new Text("7\ttest"));
		rows.add(new Text("8\ttest"));
		rows.add(new Text("9\ttest"));
		rows.add(new Text("10\ttest"));

		for (int i = 0; i < rows.size() / 2; i++) {
			JdbcOutputWritable output = new JdbcOutputWritable(rows.get(i)
					.toString().split("\t"), outputSchema.getColumnTypes(),
					outputSchema.getPreparedStatementTypeMapping());
			rw1.write(output, NullWritable.get());
		}

		for (int i = rows.size() / 2; i < rows.size(); i++) {
			JdbcOutputWritable output = new JdbcOutputWritable(rows.get(i)
					.toString().split("\t"), outputSchema.getColumnTypes(),
					outputSchema.getPreparedStatementTypeMapping());
			rw2.write(output, NullWritable.get());
		}

		rw1.close(ctx1);
		rw2.close(ctx2);

		JdbcOutputFormat.finalizeOutput(outputSchema.getConf());

		Statement resultStatement = connection.createStatement();
		String resultQuery = "Select * from test";

		int rowcount = 0;
		ResultSet rs = resultStatement.executeQuery(resultQuery);

		while (rs.next()) {
			Text output = new Text(rs.getInt(1) + "\t" + rs.getString(2));
			assertEquals(rows.get(rowcount).toString(), output.toString());
			rowcount++;

		}

		assertEquals(rows.size(), rowcount);

	}

	@Test
	public void testDatabaseExportWithSingleWriter() throws IOException,
			SQLException, InterruptedException, ClassNotFoundException {
		JdbcOutputFormat.setOutput(conf, CONNECTIONSTRING, null, null,
				OUTPUTTABLE, null, 1, COMMITSIZE, null, null, COLUMNNAMES, COLUMNTYPES);
		Schema outputSchema = SchemaFactory.getSchema(conf);
		Connection connection = outputSchema.getConnection();
		TaskAttemptContext ctx = new TaskAttemptContextImpl(conf,
				new TaskAttemptID(new TaskID(new JobID("singleReducer", 1),
						TaskType.REDUCE, 0), 1));
		RecordWriter<JdbcOutputWritable, NullWritable> rw = outputFormat
				.getRecordWriter(ctx);

		List<Text> rows = new ArrayList<>();

		rows.add(new Text("1\ttest"));
		rows.add(new Text("2\ttest"));
		rows.add(new Text("3\ttest"));
		rows.add(new Text("4\ttest"));
		rows.add(new Text("5\ttest"));
		rows.add(new Text("6\ttest"));
		rows.add(new Text("7\ttest"));
		rows.add(new Text("8\ttest"));
		rows.add(new Text("9\ttest"));
		rows.add(new Text("10\ttest"));

		for (int i = 0; i < rows.size(); i++) {
			JdbcOutputWritable output = new JdbcOutputWritable(rows.get(i)
					.toString().split("\t"), outputSchema.getColumnTypes(),
					outputSchema.getPreparedStatementTypeMapping());
			rw.write(output, NullWritable.get());
		}

		rw.close(ctx);

		JdbcOutputFormat.finalizeOutput(outputSchema.getConf());

		Statement resultStatement = connection.createStatement();
		String resultQuery = "Select * from test";

		int rowcount = 0;
		ResultSet rs = resultStatement.executeQuery(resultQuery);

		while (rs.next()) {
			Text output = new Text(rs.getInt(1) + "\t" + rs.getString(2));
			assertEquals(rows.get(rowcount).toString(), output.toString());
			rowcount++;

		}

		assertEquals(rows.size(), rowcount);

	}

	@Test
	public void testDatabaseTemporaryExportWithSingleWriter()
			throws IOException, SQLException, InterruptedException,
			ClassNotFoundException {
		JdbcOutputFormat.setOutput(conf, CONNECTIONSTRING, null, null,
				OUTPUTTABLE, null, 1, COMMITSIZE, null, null, COLUMNNAMES, COLUMNTYPES);
		Schema outputSchema = SchemaFactory.getSchema(conf);
		Connection connection = outputSchema.getConnection();
		TaskAttemptContext ctx = new TaskAttemptContextImpl(conf,
				new TaskAttemptID(new TaskID(new JobID("singleReducer", 1),
						TaskType.REDUCE, 0), 1));
		RecordWriter<JdbcOutputWritable, NullWritable> rw = outputFormat
				.getRecordWriter(ctx);

		List<Text> rows = new ArrayList<>();

		rows.add(new Text("1\ttest"));
		rows.add(new Text("2\ttest"));
		rows.add(new Text("3\ttest"));
		rows.add(new Text("4\ttest"));
		rows.add(new Text("5\ttest"));
		rows.add(new Text("6\ttest"));
		rows.add(new Text("7\ttest"));
		rows.add(new Text("8\ttest"));
		rows.add(new Text("9\ttest"));
		rows.add(new Text("10\ttest"));

		for (int i = 0; i < rows.size(); i++) {
			JdbcOutputWritable output = new JdbcOutputWritable(rows.get(i)
					.toString().split("\t"), outputSchema.getColumnTypes(),
					outputSchema.getPreparedStatementTypeMapping());
			rw.write(output, NullWritable.get());
		}

		rw.close(ctx);

		Statement resultStatement = connection.createStatement();
		String resultQuery = "Select * from tmp_test_"
				+ ctx.getTaskAttemptID().getTaskID().getId();

		int rowcount = 0;
		ResultSet rs = resultStatement.executeQuery(resultQuery);

		while (rs.next()) {
			Text output = new Text(rs.getInt(1) + "\t" + rs.getString(2));
			assertEquals(rows.get(rowcount).toString(), output.toString());
			rowcount++;

		}

		assertEquals(rows.size(), rowcount);

	}

}
