package org.schedoscope.export.outputschema;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class DerbySchemaTest {

	private Configuration conf = new Configuration();
	private DerbySchema schema;

	private static final String[] COLUMN_NAMES= new String[] {"id", "name"};
	private static final String[] COLUMN_TYPES= new String[] {"int", "string"};
	private static final String TABLE_NAME = "derby_test";
	private static final int NUM_PARTITIONS = 1;
	private static final int COMMIT_SIZE = 1000;


	@Before
	public void setUp() {
		schema = new DerbySchema(conf);
		schema.setOutput("jdbc:derby:memory:TestingDB", "user", "pass", TABLE_NAME, null, NUM_PARTITIONS, COMMIT_SIZE, COLUMN_NAMES, COLUMN_TYPES);
	}

	@Test
	public void testGetTable() {
		assertEquals(TABLE_NAME, schema.getTable());
	}

	@Test
	public void testGetColumnNames() {
		assertArrayEquals(COLUMN_NAMES, schema.getColumnNames());
	}

	@Test
	public void testGetColumnTypes() {
		assertArrayEquals(COLUMN_TYPES, schema.getColumnTypes());
	}

	@Test
	public void testGetNumberOfPartitions() {
		assertEquals(NUM_PARTITIONS, schema.getNumberOfPartitions());
	}

	@Test
	public void testGetCommitSize() {
		assertEquals(COMMIT_SIZE, schema.getCommitSize());
	}

	@Test
	public void testGetFilter() {
		assertEquals(null, schema.getFilter());
	}

	@Test
	public void testBuildCreateTableStatememt() {
		assertThat(schema.getCreateTableQuery(), allOf(
				containsString("CREATE"),
				containsString(TABLE_NAME),
				containsString("id"),
				containsString("int"),
				containsString("name"),
				containsString("string")));

	}
}
