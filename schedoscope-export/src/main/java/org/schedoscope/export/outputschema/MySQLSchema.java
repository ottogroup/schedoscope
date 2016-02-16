package org.schedoscope.export.outputschema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class MySQLSchema extends AbstractSchema implements Schema {

	protected static final String JDBC_DRIVER_NAME = "com.mysql.jdbc.Driver";

	@SuppressWarnings("serial")
	private static final Map<String, String> columnTypeMapping = Collections.unmodifiableMap(
			new HashMap<String, String>() {
				{
					put("string", "text");
					put("boolean", "boolean");
					put("int", "int");
					put("long", "bigint");
					put("bigint", "bigint");
					put("double", "double");
					put("float", "float");
					put("tinyint", "tinyint");
				}
			});

	@SuppressWarnings("serial")
	private static final Map<String, String> preparedStatementTypeMapping = Collections.unmodifiableMap(
			new HashMap<String, String>() {
				{
					put("text", "string");
					put("boolean", "boolean");
					put("int", "int");
					put("bigint", "long");
					put("double", "double");
					put("float", "float");
					put("tinyint", "int");
				}
			});

	public MySQLSchema(Configuration conf) {
		super(conf);
		this.conf.set(Schema.JDBC_DRIVER_CLASS, JDBC_DRIVER_NAME);

	}

	@Override
	public Map<String, String> getColumnTypeMapping() {
		return columnTypeMapping;
	}

	@Override
	public Map<String, String> getPreparedStatementTypeMapping() {
		return preparedStatementTypeMapping;
	}
}
