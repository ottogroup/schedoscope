package org.schedoscope.export.outputschema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class ExasolSchemaTest {

	private Configuration conf = new Configuration();
	private ExasolSchema schema;
	
	private static final String[] COLUMN_NAMES= new String[] {"username", "pass"};
	private static final String[] COLUMN_TYPES= new String[] {"boolean", "int"};
	private static final String TABLE_NAME = "exasol_test";
	private static final int NUM_PARTITIONS = 5;
	private static final int COMMIT_SIZE = 25000;
	
	@Before
	public void setUp() {
		schema = new ExasolSchema(conf);
		schema.setOutput("jdbc:exa:10.15.101.11..13:8563;schema=USR_TESTER_EXASOL", "user", "pass", TABLE_NAME, null, NUM_PARTITIONS, COMMIT_SIZE, COLUMN_NAMES, COLUMN_TYPES);
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
}
