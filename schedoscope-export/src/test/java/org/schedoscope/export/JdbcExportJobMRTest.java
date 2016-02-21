package org.schedoscope.export;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.outputformat.JdbcOutputWritable;

public class JdbcExportJobMRTest extends HiveUnitBaseTest {

	MapDriver<WritableComparable<?>, HCatRecord, Text, NullWritable> mapDriver;
	ReduceDriver<Text, NullWritable, JdbcOutputWritable, NullWritable> reduceDriver;
	MapReduceDriver<WritableComparable<?>, HCatRecord, Text, NullWritable, JdbcOutputWritable, NullWritable> mapReduceDriver;

	@SuppressWarnings("deprecation")
	@Before
	public void setUp() throws Exception {
		super.setUp();
		JdbcExportMapper mapper = new JdbcExportMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		mapDriver.setConfiguration(conf);

		JdbcExportReducer reducer = new JdbcExportReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		reduceDriver.setConfiguration(conf);

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.setConfiguration(conf);

		setUpHiveServer("src/test/resources/ogm_event_features_data.txt",
				"src/test/resources/ogm_event_features.hql",
				"ogm_event_features");
	}

	@Test
	public void testJdbcMapper() throws Exception {

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {
		HCatRecord record = it.next();
			mapDriver.withInput(NullWritable.get(), record);
		}
		List<Pair<Text, NullWritable>> out = mapDriver.run();
		assertEquals(10, out.size());

		String result = out.get(0).toString().split("\t")[3];
		System.err.println(out.get(0));
		assertEquals("app.eci.datahub.OgmEventFeatures", result);
	}

	@Test
	public void testMapReduce() throws IOException {

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {
		HCatRecord record = it.next();
			mapReduceDriver.withInput(NullWritable.get(), record);
		}
		List<Pair<JdbcOutputWritable, NullWritable>> out = mapReduceDriver.run();
		assertEquals(10, out.size());

	}

	@Test
	public void testReducer() throws IOException{

		// input data
		Text key = new Text("1\ttest");
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());

		reduceDriver.withInput(key, values);
		List<Pair<JdbcOutputWritable, NullWritable>> out = reduceDriver.run();
		// assertEquals(10, out.size());

		//JdbcOutputWritable outWritable = out.get(0).getFirst();
		// assertNotNull(outWritable);
	}
}

