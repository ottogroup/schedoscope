package org.schedoscope.export.outputschema;

import org.apache.hadoop.conf.Configuration;

public class SchemaFactory {

	public static Schema getSchema(String dbConnectionString, Configuration conf) {
		String dialect = getDialect(dbConnectionString);
		if (dialect.equals("exa")) {
			return new ExasolSchema(conf);
		} else if (dialect.equals("derby")) {
			return new DerbySchema(conf);
		} else {
			throw new IllegalArgumentException(dbConnectionString + " not a valid jdbc connection string");
		}
	}

	public static Schema getSchema(Configuration conf) {
		String dbConnectionString = conf.get(Schema.JDBC_CONNECTION_STRING);
		return getSchema(dbConnectionString, conf);
	}

	private static String getDialect(String dbConnectionString) {
		String[] parts = dbConnectionString.split(":");
		if (parts.length >= 2 && !parts[1].equals("")) {
			return parts[1];
		}
		throw new IllegalArgumentException(dbConnectionString + " not a valid jdbc connection string");
	}
}
