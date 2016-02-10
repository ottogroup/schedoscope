package org.schedoscope.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;

public class JdbcExportMRTest {

	ReduceDriver<Text, NullWritable, JdbcOutputWritable, NullWritable> reduceDriver;
	String[] columnTypes = new String[] {"int", "string"};
	Schema outputSchema;

	@Before
	public void setUp() {

		JdbcExportReducer reducer = new JdbcExportReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		Configuration conf = reduceDriver.getConfiguration();
		conf.set(Schema.JDBC_CONNECTION_STRING, "jdbc:exa:10.15.101.11..13:8563;schema=XXXX");
		conf.setStrings(Schema.JDBC_OUTPUT_COLUMN_TYPES, columnTypes);

		outputSchema = SchemaFactory.getSchema(conf);
	}

	@Test
	public void testReducer() throws IOException{

		// input data
		Text key = new Text("1\ttest");
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());

		reduceDriver.withInput(key, values);
		List<Pair<JdbcOutputWritable, NullWritable>> out = reduceDriver.run();
		JdbcOutputWritable outWritable = out.get(0).getFirst();
	}
}
