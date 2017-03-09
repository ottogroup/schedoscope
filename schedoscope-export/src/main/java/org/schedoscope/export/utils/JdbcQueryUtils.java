/**
 * Copyright 2016 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.utils;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A utility class to generate and execute various database related statements
 * to drop, create tables and insert data into them. Uses JDBC
 * {@link java.sql.Connection Connection} and {@link java.sql.Statement
 * Statement}.
 */
public class JdbcQueryUtils {

    private static final Log LOG = LogFactory.getLog(JdbcQueryUtils.class);

    /**
     * Drops a table from a database underneath the connection object.
     *
     * @param table      The table to drop.
     * @param connection The JDBC connection to use.
     */
    public static void dropTable(String table, Connection connection) {

        table = table.replace(";", "");

        StringBuilder dropTableQuery = new StringBuilder();
        dropTableQuery.append("DROP TABLE ");
        dropTableQuery.append(table);

        LOG.info("Drop Table: ");
        LOG.info(dropTableQuery);

        executeStatementIfExists(dropTableQuery.toString(), connection);
    }

    /**
     * Drop multiple temporary tables from a database underneath a connection
     * object.
     *
     * @param table              The table to drop.
     * @param numberOfPartitions The number of partitions, each partition represents a table.
     * @param connection         The JDBC connection to use.
     */
    public static void dropTemporaryOutputTables(String table,
                                                 int numberOfPartitions, Connection connection) {

        table = table.replace(";", "");

        for (int i = 0; i < numberOfPartitions; i++) {

            StringBuilder dropTableQuery = new StringBuilder();
            dropTableQuery.append("DROP TABLE ");
            dropTableQuery.append(table);
            dropTableQuery.append("_");
            dropTableQuery.append(i);

            LOG.info("Drop Table: ");
            LOG.info(dropTableQuery);

            executeStatementIfExists(dropTableQuery.toString(), connection);

        }
    }

    /**
     * Deletes existing rows from a given table, conditions are passed in as
     * well
     *
     * @param table      The table from which to delete rows.
     * @param filter     The filter condition.
     * @param connection The JDBC connection object.
     */
    public static void deleteExisitingRows(String table, String filter,
                                           Connection connection) {

        table = table.replace(";", "");
        filter = filter.replace(";", "");

        StringBuilder deleteRowsQuery = new StringBuilder();
        deleteRowsQuery.append("DELETE FROM ");
        deleteRowsQuery.append(table);
        deleteRowsQuery.append(" WHERE USED_FILTER='");
        deleteRowsQuery.append(filter);
        deleteRowsQuery.append("'");

        LOG.info("Delete rows: ");
        LOG.info(deleteRowsQuery);

        executeStatementIfExists(deleteRowsQuery.toString(), connection);
    }

    /**
     * Merges multiple temporary tables into the final output table, structure
     * must be the same, uses "UNION ALL" for merging.
     *
     * @param table              The final table containing the merged result.
     * @param tablePrefix        The prefix to prepend to the output table name.
     * @param numberOfPartitions The number of temporary tables to merge.
     * @param connection         The JDBC connection object.
     */
    public static void mergeOutput(String table, String tablePrefix,
                                   int numberOfPartitions, Connection connection) {

        StringBuilder mergeOutputQuery = new StringBuilder();
        mergeOutputQuery.append("INSERT INTO ");
        mergeOutputQuery.append(table);

        for (int i = 0; i < numberOfPartitions; i++) {
            mergeOutputQuery.append("\n");
            mergeOutputQuery.append("SELECT * FROM ");
            mergeOutputQuery.append(tablePrefix);
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

        executeStatement(mergeOutputQuery.toString(), connection);
    }

    /**
     * Executes a given CREATE TABLE ... statement.
     *
     * @param createTableQuery The SQL query to execute
     * @param connection       The JDBC connection object.
     */
    public static void createTable(String createTableQuery,
                                   Connection connection) {

        LOG.info("Create Table from DDL:");
        LOG.info(createTableQuery);

        executeStatement(createTableQuery, connection);
    }

    /**
     * Creates a prepared statement to insert data into a table.
     *
     * @param table      The table to insert the data into.
     * @param fieldNames An array with the column names.
     * @return The final SQL statement to insert data via a JDBC statement.
     */
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
        insertQuery.append(")");

        LOG.info("Insert into: ");
        LOG.info(insertQuery.toString());

        return insertQuery.toString();
    }

    private static void executeStatementIfExists(String query, Connection connection) {
        try {
            executeStatementWithoutErrorHandling(query, connection);
        } catch (SQLException se) {
            if (se.getMessage().contains("does not exist.")) {
                LOG.info("error executing statement:" + se.getMessage());
            } else {
                LOG.error("error executing statement:" + se.getMessage());
            }

        }
    }

    private static void executeStatement(String query, Connection connection) {
        try {
            executeStatementWithoutErrorHandling(query, connection);
        } catch (SQLException se) {
            LOG.error("error executing statement:" + query + "\n" + se.getMessage());

        }
    }

    private static void executeStatementWithoutErrorHandling(String query, Connection connection) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query);
        } finally {
            DbUtils.closeQuietly(statement);
        }
    }
}
