package org.schedoscope.export.outputschema;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SchemaFactoryTest {

	private static final String EXASOL_CONNECT_STRING = "jdbc:exa:10.15.101.11..13:8563;schema=XXXX";
	private static final String DERBY_CONNECT_STRING = "jdbc:derby:memory:TestingDB";
	Configuration conf = new Configuration();

	@Test
	public void testGetDerbySchemaByConf() {
		conf.set(Schema.JDBC_CONNECTION_STRING, DERBY_CONNECT_STRING);
		Schema schema = SchemaFactory.getSchema(conf);
		assertThat(schema, instanceOf(DerbySchema.class));
	}

	@Test
	public void testGetDerbxSchemaByString() {
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
