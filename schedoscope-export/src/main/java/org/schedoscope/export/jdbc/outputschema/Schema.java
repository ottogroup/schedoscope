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

package org.schedoscope.export.jdbc.outputschema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * A schema is a collection of functions and properties to write data into a particular database technology, e.g. mysql,
 * postgresql, etc. It hold information about database connection settings and creates the SQL statements, provides the
 * JDBC connection.
 *
 */
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
    public static final String JDBC_MYSQL_STORAGE_ENGINE = "jdbc.mysql.storage.engine";
    public static final String JDBC_EXASOL_DISTRIBUTED_CLAUSE = "jdbc.exasol.distributed.clause";

    /**
     * Initializes a {@link Schema} with the given
     * parameter.
     *
     * @param connectionString The JDBC connection string.
     * @param username The database user name.
     * @param password The database password.
     * @param outputTable The table name.
     * @param inputFilter The input filter (optional).
     * @param outputNumberOfPartitions The number of partitions.
     * @param outputCommitSize The commit size.
     * @param storageEngine The storage engine for MySQL.
     * @param distributedBy An optional clause for Exasol.
     * @param columnNames The column names.
     * @param columnsTypes The column types.
     */
    public void setOutput(String connectionString,
            String username, String password, String outputTable,
            String inputFilter, int outputNumberOfPartitions,
            int outputCommitSize, String storageEngine,
            String distributedBy, String[] columnNames, String[] columnsTypes);

    /**
     * Returns the table name.
     *
     * @return The name of the table.
     */
    public String getTable();

    /**
     * Returns the names of all columns that are used
     * to build the SQL statements.
     *
     * @return The column names.
     */
    public String[] getColumnNames();

    /**
     * Returns the types of all columns that are used
     * to build the SQL statement.
     *
     * @return The column types.
     */
    public String[] getColumnTypes();

    /**
     * Returns the name mapping. The name mapping is used
     * to use different column names if required.
     *
     * @return The name mapping
     */
    public Map<String, String> getColumnNameMapping();

    /**
     * Returns the type mapping for the column data
     * types. This map contains a mapping between
     * data types of different database dialects.
     *
     * @return A map containing the type mappings
     */
    public Map<String, String> getColumnTypeMapping();

    /**
     * Returns the type mapping for the prepared
     * statement. This map contains a mapping between
     * data types of different database dialects.
     *
     * @return The type map.
     */
    public Map<String, String> getPreparedStatementTypeMapping();

    /**
     * Returns the underlying JDBC connection object.
     *
     * @return The JDBC connection.
     * @throws ClassNotFoundException Exception thrown if JDBC driver can't be initialized.
     * @throws SQLException Exception thrown if SQL error occurs.
     */
    public Connection getConnection() throws ClassNotFoundException, SQLException;

    /**
     * Returns the create table statement.
     *
     * @return Create table statement.
     */
    public String getCreateTableQuery();

    /**
     * Returns the number of partitons, defines
     * how many JDBC database writer are running
     * in parallel.
     *
     * @return Number of partitions.
     */
    public int getNumberOfPartitions();

    /**
     * Returns the commit size, that is the
     * number of records to insert within a
     * single transaction.
     *
     * @return The commit size.
     */
    public int getCommitSize();

    /**
     * Returns the currently used filter.
     *
     * @return The filter used.
     */
    public String getFilter();

    /**
     * Returns the Hadoop configuration object.
     *
     * @return The configuration object.
     */
    public Configuration getConf();

    /**
     * Returns the JDBC driver name.
     *
     * @return The JDBC driver name.
     */
    public String getDriverName();
}
