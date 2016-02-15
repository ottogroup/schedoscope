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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SchemaFactoryTest {

	private static final String EXASOL_CONNECT_STRING = "jdbc:exa:10.15.101.11..13:8563;schema=XXXX";
	private static final String DERBY_CONNECT_STRING = "jdbc:derby:memory:TestingDB";
	private static final String MYSQL_CONNECT_STRING = "jdbc:mysql://remotehost/nonexistent";

	Configuration conf = new Configuration();

	@Test
	public void testGetDerbySchemaByConf() {
		conf.set(Schema.JDBC_CONNECTION_STRING, DERBY_CONNECT_STRING);
		Schema schema = SchemaFactory.getSchema(conf);
		assertThat(schema, instanceOf(DerbySchema.class));
	}

	@Test
	public void testGetDerbySchemaByString() {
		Schema schema = SchemaFactory.getSchema(DERBY_CONNECT_STRING, conf);
		assertThat(schema, instanceOf(DerbySchema.class));
	}

	@Test
	public void testGetExasolSchemaByConf() {
		conf.set(Schema.JDBC_CONNECTION_STRING, EXASOL_CONNECT_STRING);
		Schema schema = SchemaFactory.getSchema(conf);
		assertThat(schema, instanceOf(ExasolSchema.class));
	}

	@Test
	public void testGetExasolSchemaByString() {
		Schema schema = SchemaFactory.getSchema(EXASOL_CONNECT_STRING, conf);
		assertThat(schema, instanceOf(ExasolSchema.class));
	}

	@Test
	public void testGetMySQLSchemaByConf() {
		conf.set(Schema.JDBC_CONNECTION_STRING, MYSQL_CONNECT_STRING);
		Schema schema = SchemaFactory.getSchema(conf);
		assertThat(schema, instanceOf(MySQLSchema.class));
	}

	@Test
	public void testGetMySQLSchemaByString() {
		Schema schema = SchemaFactory.getSchema(MYSQL_CONNECT_STRING, conf);
		assertThat(schema, instanceOf(MySQLSchema.class));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testGetSchemaInvalidConnectString1() {
		SchemaFactory.getSchema("jdbc:dery:memory", conf);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testGetSchemaInvalidConnectString2() {
		SchemaFactory.getSchema("jdbc:mysqll", conf);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testGetSchemaInvalidConnectString3() {
		SchemaFactory.getSchema("", conf);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testGetSchemaInvalidConnectString4() {
		SchemaFactory.getSchema("jdbc:dery:memory", conf);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testGetSchemaInvalidConnectString5() {
		SchemaFactory.getSchema("jdbc", conf);
	}
}
