package org.schedoscope.export.outputschema;

import org.apache.hadoop.conf.Configuration;

public class SchemaFactory {

	public static Schema getSchema(String driver, Configuration conf) {
		if (driver.contains("exasol")) {
			return new ExasolSchema(conf);
		} else if (driver.contains("derby")) {
			return new DerbySchema(conf);
		} else {
			return null;
		}

	}

	public static Schema getSchema(Configuration conf) {
		String driver = conf.get(Schema.JDBC_DRIVER_CLASS);
		if (driver.contains("exasol")) {
			return new ExasolSchema(conf);
		} else if (driver.contains("derby")) {
			return new DerbySchema(conf);
		} else {
			return null;
		}

	}
}
