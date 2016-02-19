package org.schedoscope.export;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.After;
import org.junit.Before;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;
import org.schedoscope.export.outputschema.SchemaUtils;

import com.inmobi.hive.test.HiveTestSuite;

public abstract class HiveUnitBaseTest {

	private static final String DEFAUlT_HIVE_DB = "default";
	private static final String DEFAULT_DERBY_DB = "jdbc:derby:memory:TestingDB;create=true";
	private static final String DATA_FILE_PATH = "DATA_FILE_PATH";

	private HiveTestSuite testSuite;
	private HCatSchema hcatInputSchema;

	// use those two instances to set up
	// the unit test, the conf is needed
	// to pass to the map and reduce mrunit
	// driver and the hcatRecordReader can
	// be used to set up the map driver with
	// input data, please see example.
	protected HCatReader hcatRecordReader;
	protected Configuration conf;


	@Before
	public void setUp()  throws IOException {
		testSuite = new HiveTestSuite();
		testSuite.createTestCluster();
		conf = testSuite.getFS().getConf();
	}

	@After
	public void testHCatInputFormat() {
		testSuite.shutdownTestCluster();
	}


	public void setUpHiveServer(String dataFile, String hiveScript, String tableName) throws IOException {

		// load data into hive table
		File inputRawData = new File(dataFile);
		String inputRawDataAbsFilePath = inputRawData.getAbsolutePath();
		Map<String, String> params = new HashMap<String, String>();
		params.put(DATA_FILE_PATH, inputRawDataAbsFilePath);
		List<String> results = testSuite.executeScript(hiveScript, params);
		assertNotEquals(0, results.size());


		// set up database related settings
		Configuration conf = testSuite.getFS().getConf();
		conf.set(Schema.JDBC_CONNECTION_STRING, DEFAULT_DERBY_DB);
		Schema schema = SchemaFactory.getSchema(conf);

		// set up column type mapping
		HCatInputFormat.setInput(conf, DEFAUlT_HIVE_DB, tableName);
		hcatInputSchema = HCatInputFormat.getTableSchema(conf);
		conf.setStrings(Schema.JDBC_OUTPUT_COLUMN_TYPES,SchemaUtils.getColumnTypesFromHcatSchema(hcatInputSchema, schema));

		// set up hcatalog record reader
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase(DEFAUlT_HIVE_DB).withTable(tableName).build();

		Map<String, String> config = new HashMap<String, String>();
		HCatReader masterReader = DataTransferFactory.getHCatReader(entity, config);
		ReaderContext ctx = masterReader.prepareRead();

		hcatRecordReader = DataTransferFactory.getHCatReader(ctx, 0);
	}
}
