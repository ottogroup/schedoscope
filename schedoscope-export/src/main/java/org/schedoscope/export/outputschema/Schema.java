package org.schedoscope.export.outputschema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface Schema {

	public static final String JDBC_DRIVER_CLASS = "jdbc.driver.class";
	public static final String JDBC_CONNECTION_STRING = "jdbc.connection.string";
	public static final String JDBC_USERNAME = "jdbc.username";
	public static final String JDBC_PASSWORD = "jdbc.password";
	public static final String JDBC_OUTPUT_TABLE = "jdbc.output.table";
	public static final String JDBC_CREATE_TABLE_QUERY = "jdbc.create.table.query";
	public static final String JDBC_INPUT_FILTER = "jdbc.input.filter";
	public static final String JDBC_NUMBER_OF_PARTITIONS = "jdbc.number.of.partitions";
	public static final String JDBC_COMMIT_SIZE = "jdbc.commit.size";
	public static final String JDBC_OUTPUT_COLUMN_NAMES = "jdbc.output.column.names";
	public static final String JDBC_OUTPUT_COLUMN_TYPES = "jdbc.output.column.types";

	public void setOutput(String driver, String connectionString,
			String username, String password, String outputTable,
			String inputFilter, int outputNumberOfPartitions,
			int outputCommitSize, String[] columnNames, String[] columnsTypes);

	public String getTable();

	public String[] getColumnNames();

	public String[] getColumnTypes();

	public Map<String, String> getColumnNameMapping();

	public Map<String, String> getColumnTypeMapping();

	public Map<String, String> getPreparedStatementTypeMapping();

	public Connection getConnection() throws ClassNotFoundException,
			SQLException;

	public String getCreateTableQuery();

	public int getNumberOfPartitions();

	public int getCommitSize();

	public String getFilter();

}
