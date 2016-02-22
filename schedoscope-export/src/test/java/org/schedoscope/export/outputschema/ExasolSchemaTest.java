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
package org.schedoscope.export.outputschema;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
		schema.setOutput("jdbc:exa:10.15.101.11..13:8563;schema=USR_TESTER_EXASOL", "user", "pass", TABLE_NAME, null, NUM_PARTITIONS, COMMIT_SIZE, null, null, COLUMN_NAMES, COLUMN_TYPES);
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
				containsString("username"),
				containsString("boolean"),
				containsString("pass"),
				containsString("int")));
	}

//	@Test
//	public void testGetConnection() throws SQLException, ClassNotFoundException {
//		assertNotNull(schema.getConnection());
//	}
}
