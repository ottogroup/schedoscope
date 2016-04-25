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

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.After;
import org.junit.Before;
import org.schedoscope.export.jdbc.outputschema.Schema;
import org.schedoscope.export.jdbc.outputschema.SchemaFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.inmobi.hive.test.HiveTestSuite;

public abstract class HiveUnitBaseTest {

	// Remove logging noise because of Jersey in Mini Hadoop test cluster
	static {
		LogManager.getLogManager().reset();
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
		Logger.getLogger("global").setLevel(Level.FINEST);
	}

	private static final String DEFAUlT_HIVE_DB = "default";
	private static final String DEFAULT_DERBY_DB = "jdbc:derby:memory:TestingDB;create=true";
	private static final String DATA_FILE_PATH = "DATA_FILE_PATH";

	private HiveTestSuite testSuite;

	// use those two instances to set up
	// the unit test, the conf is needed
	// to pass to the map and reduce mrunit
	// driver and the hcatRecordReader can
	// be used to set up the map driver with
	// input data, please see example.
	protected HCatReader hcatRecordReader;
	protected Configuration conf;
	protected HCatSchema hcatInputSchema;

	@Before
	public void setUp() throws Exception {
		testSuite = new HiveTestSuite();
		testSuite.createTestCluster();
		conf = testSuite.getFS().getConf();
	}

	@After
	public void tearDown() throws Exception {
		testSuite.shutdownTestCluster();
	}

	public void setUpHiveServer(String dataFile, String hiveScript,
			String tableName) throws Exception {

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
		// conf.setStrings(Schema.JDBC_OUTPUT_COLUMN_TYPES, SchemaUtils
		// 		.getColumnTypesFromHcatSchema(hcatInputSchema, schema));

		// set up hcatalog record reader
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase(DEFAUlT_HIVE_DB)
				.withTable(tableName).build();

		Map<String, String> config = new HashMap<String, String>();
		HCatReader masterReader = DataTransferFactory.getHCatReader(entity,
				config);
		ReaderContext ctx = masterReader.prepareRead();

		hcatRecordReader = DataTransferFactory.getHCatReader(ctx, 0);
	}

	public void setUpHiveServerNoData(String hiveScript, String tableName)
			throws Exception {

		// load data into hive table
		testSuite.executeScript(hiveScript);

		HCatInputFormat.setInput(conf, DEFAUlT_HIVE_DB, tableName);
		hcatInputSchema = HCatInputFormat.getTableSchema(conf);
	}
}
