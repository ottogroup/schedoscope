package org.schedoscope.export.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JdbcQueryUtils {

	private static final Log LOG = LogFactory.getLog(JdbcQueryUtils.class);

	public static void dropTemporaryOutputTable(String table,
			Connection connection) {
		Statement statement = null;
		try {

			statement = connection.createStatement();

			table = table.replace(";", "");

			StringBuilder dropTableQuery = new StringBuilder();
			dropTableQuery.append("DROP TABLE ");
			dropTableQuery.append(table);
			dropTableQuery.append(";");

			LOG.info("Drop Table: ");
			LOG.info(dropTableQuery);

			statement.executeUpdate(dropTableQuery.toString());

		} catch (SQLException se) {

		} finally {
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException se2) {
			}
		}

	}

	public static void dropTemporaryOutputTables(String table,
			int numberOfPartitions, Connection connection) {
		Statement statement = null;
		table = table.replace(";", "");

		try {

			for (int i = 0; i < numberOfPartitions; i++) {
				statement = connection.createStatement();

				StringBuilder dropTableQuery = new StringBuilder();
				dropTableQuery.append("DROP TABLE ");
				dropTableQuery.append(table);
				dropTableQuery.append("_");
				dropTableQuery.append(i);
				dropTableQuery.append(";");

				LOG.info("Drop Table: ");
				LOG.info(dropTableQuery);

				statement.executeUpdate(dropTableQuery.toString());
			}

		} catch (SQLException se) {

		} finally {
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException se2) {
			}
		}

	}

	public static void deleteExisitingRows(String table, String filter,
			Connection connection) {
		Statement statement = null;
		try {

			statement = connection.createStatement();

			table = table.replace(";", "");
			filter = filter.replace(";", "");

			StringBuilder deleteRowsQuery = new StringBuilder();
			deleteRowsQuery.append("DELETE FROM ");
			deleteRowsQuery.append(table);
			deleteRowsQuery.append(" WHERE USED_FILTER='");
			deleteRowsQuery.append(filter);
			deleteRowsQuery.append("';");

			LOG.info("Delete rows: ");
			LOG.info(deleteRowsQuery);

			statement.executeUpdate(deleteRowsQuery.toString());

		} catch (SQLException se) {

		} finally {
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException se2) {
			}
		}

	}

	public static void mergeOutput(String table, int numberOfPartitions,
			Connection connection) {
		Statement statement = null;
		try {
			statement = connection.createStatement();

			StringBuilder mergeOutputQuery = new StringBuilder();
			mergeOutputQuery.append("INSERT INTO ");
			mergeOutputQuery.append(table);

			for (int i = 0; i < numberOfPartitions; i++) {
				mergeOutputQuery.append("\n");
				mergeOutputQuery.append("SELECT * FROM tmp_");
				mergeOutputQuery.append(table);
				mergeOutputQuery.append("_");
				mergeOutputQuery.append(i);
				if (i != numberOfPartitions - 1) {
					mergeOutputQuery.append("\n");
					mergeOutputQuery.append("UNION ALL");
				}
			}

			LOG.info("Merge output: ");
			LOG.info(mergeOutputQuery);

			statement.executeUpdate(mergeOutputQuery.toString());

		} catch (SQLException se) {

		} finally {
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException se2) {
			}
		}

	}

	public static void createTable(String createTableQuery,
			Connection connection) {

		Statement statement = null;
		try {
			statement = connection.createStatement();

			LOG.info("Create Table from DDL:");
			LOG.info(createTableQuery);

			statement.executeUpdate(createTableQuery);

		} catch (SQLException se) {

		} finally {
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException se2) {
			}
		}
	}

	public static String createInsertQuery(String table, String[] fieldNames) {
		if (fieldNames == null) {
			throw new IllegalArgumentException("Field names may not be null");
		}

		StringBuilder insertQuery = new StringBuilder();
		insertQuery.append("INSERT INTO ");
		insertQuery.append(table);

		if (fieldNames.length > 0 && fieldNames[0] != null) {
			insertQuery.append(" (");
			for (int i = 0; i < fieldNames.length; i++) {
				insertQuery.append(fieldNames[i]);
				if (i != fieldNames.length - 1) {
					insertQuery.append(",");
				}
			}
			insertQuery.append(")");
		}
		insertQuery.append(" VALUES (");

		for (int i = 0; i < fieldNames.length; i++) {
			insertQuery.append("?");
			if (i != fieldNames.length - 1) {
				insertQuery.append(",");
			}
		}
		insertQuery.append(");");

		LOG.info("Insert into: ");
		LOG.info(insertQuery.toString());

		return insertQuery.toString();
	}

}
