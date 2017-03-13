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

package org.schedoscope.export.jdbc.outputformat;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.schedoscope.export.jdbc.exception.RetryException;
import org.schedoscope.export.jdbc.exception.UnrecoverableException;
import org.schedoscope.export.jdbc.outputschema.Schema;
import org.schedoscope.export.jdbc.outputschema.SchemaFactory;
import org.schedoscope.export.utils.JdbcQueryUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The JDBC output format is responsible to write data into a database using
 * JDBC connection.
 *
 * @param <K> The key class.
 * @param <V> The value class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JdbcOutputFormat<K, V extends DBWritable> extends
        OutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(JdbcOutputFormat.class);

    private static final String TMPDB = "TMP_";

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {

        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
    }

    /**
     * The JDBC Record Writer is used to write data into a database using a JDBC
     * connection.
     */
    @InterfaceStability.Evolving
    public class JdbcRecordWriter extends RecordWriter<K, V> {

        private Connection connection;
        private PreparedStatement statement;
        private int rowsInBatch = 0;
        private int rowsTotal = 0;
        private int commitSize = 25000;

        public JdbcRecordWriter() throws SQLException {
        }

        /**
         * The constructor to initialize the JDBC Record Writer.
         *
         * @param connection The JDBC connection.
         * @param statement  The prepared statement.
         * @param commitSize The batch size
         * @throws SQLException Is thrown if a error occurs.
         */
        public JdbcRecordWriter(Connection connection,
                                PreparedStatement statement, int commitSize)
                throws SQLException {

            this.connection = connection;
            this.statement = statement;
            this.commitSize = commitSize;
            this.connection.setAutoCommit(false);
        }

        public Connection getConnection() {

            return connection;
        }

        public PreparedStatement getStatement() {

            return statement;
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {

            try {

                if (rowsInBatch > 0) {
                    statement.executeBatch();
                }

                if (rowsTotal > 0) {
                    connection.commit();
                }

            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    LOG.warn(StringUtils.stringifyException(ex));
                }
                throw new IOException(e.getMessage());
            } finally {
                DbUtils.closeQuietly(statement);
                DbUtils.closeQuietly(connection);
            }
        }

        @Override
        public void write(K key, V value) throws IOException {

            try {
                value.write(statement);
                statement.addBatch();
                if (rowsInBatch == commitSize) {
                    statement.executeBatch();
                    rowsInBatch = 0;
                } else {
                    rowsInBatch++;
                    rowsTotal++;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
            throws IOException {

        Schema outputSchema = SchemaFactory.getSchema(context
                .getConfiguration());

        String tmpOutputTable = getTablePrefix(outputSchema) + outputSchema.getTable() + "_"
                + context.getTaskAttemptID().getTaskID().getId();
        String createTableQuery = outputSchema.getCreateTableQuery();

        createTableQuery = createTableQuery.replace(outputSchema.getTable(),
                tmpOutputTable);

        int commitSize = outputSchema.getCommitSize();
        String[] fieldNames = outputSchema.getColumnNames();

        try {
            Connection connection = outputSchema.getConnection();

            JdbcQueryUtils.dropTable(tmpOutputTable, connection);
            JdbcQueryUtils.createTable(createTableQuery, connection);

            PreparedStatement statement = null;
            statement = connection.prepareStatement(JdbcQueryUtils
                    .createInsertQuery(tmpOutputTable, fieldNames));

            return new JdbcRecordWriter(connection, statement, commitSize);

        } catch (Exception ex) {
            throw new IOException(ex.getMessage());
        }
    }

    private static String getTablePrefix(Schema outputSchema) {
        if (outputSchema.getFilter() != null) {
            String filter = outputSchema.getFilter().replace("=", "");
            return TMPDB + filter + "_";
        } else {
            return TMPDB;
        }
    }

    /**
     * Initializes the JDBCOutputFormat.
     *
     * @param conf                     The Hadoop configuration object.
     * @param connectionString         The JDBC connection string.
     * @param username                 The database user name
     * @param password                 The database password
     * @param outputTable              The output table
     * @param inputFilter              The input filter
     * @param outputNumberOfPartitions The number of partitions / reducers
     * @param outputCommitSize         The batch size
     * @param storageEngine            The storage engine, either MyISAM or InnoDB (MySQL)
     * @param distributedBy            An optional distribute by clause (Exasol)
     * @param columnNames              The column names.
     * @param columnsTypes             The column types.
     * @throws IOException Is thrown if an error occurs.
     */
    public static void setOutput(Configuration conf, String connectionString,
                                 String username, String password, String outputTable,
                                 String inputFilter, int outputNumberOfPartitions,
                                 int outputCommitSize, String storageEngine, String distributedBy,
                                 String[] columnNames, String[] columnsTypes) throws IOException {

        Schema outputSchema = SchemaFactory.getSchema(connectionString, conf);
        outputSchema.setOutput(connectionString, username, password,
                outputTable, inputFilter, outputNumberOfPartitions,
                outputCommitSize, storageEngine, distributedBy, columnNames,
                columnsTypes);
    }

    /**
     * This function finalizes the JDBC export, it merges all partitions and
     * drops the temporary tables, optionally updates the output table.
     *
     * @param conf The Hadoop configuration object.
     * @throws RetryException         Is thrown if a SQL error occurs.
     * @throws UnrecoverableException Is thrown if JDBC driver issue occurs.
     */
    public static void finalizeOutput(Configuration conf)
            throws RetryException, UnrecoverableException {

        Schema outputSchema = SchemaFactory.getSchema(conf);
        String outputTable = outputSchema.getTable();
        String tmpOutputTable = getTablePrefix(outputSchema) + outputSchema.getTable();
        String createTableStatement = outputSchema.getCreateTableQuery();
        String inputFilter = outputSchema.getFilter();
        int outputNumberOfPartitions = outputSchema.getNumberOfPartitions();

        Connection connection = null;

        try {
            connection = outputSchema.getConnection();

            if (inputFilter != null) {
                JdbcQueryUtils.deleteExisitingRows(outputTable, inputFilter,
                        connection);
            } else {
                JdbcQueryUtils.dropTable(outputTable, connection);
            }

            JdbcQueryUtils.createTable(createTableStatement, connection);
            JdbcQueryUtils.mergeOutput(outputTable, getTablePrefix(outputSchema),
                    outputNumberOfPartitions, connection);
            JdbcQueryUtils.dropTemporaryOutputTables(tmpOutputTable,
                    outputNumberOfPartitions, connection);

        } catch (SQLException ex1) {
            LOG.error(ex1.getMessage());
            throw new RetryException(ex1.getMessage());
        } catch (ClassNotFoundException ex2) {
            LOG.error(ex2.getMessage());
            throw new UnrecoverableException(ex2.getMessage());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    /**
     * This function is called if the MR job doesn't finish successfully.
     *
     * @param conf The Hadoop configuration object.
     * @throws RetryException         Is thrown if a SQL error occurs.
     * @throws UnrecoverableException Is thrown if JDBC driver issue occurs.
     */
    public static void rollback(Configuration conf) throws RetryException,
            UnrecoverableException {

        Schema outputSchema = SchemaFactory.getSchema(conf);
        String tmpOutputTable = getTablePrefix(outputSchema) + outputSchema.getTable();
        int outputNumberOfPartitions = outputSchema.getNumberOfPartitions();

        Connection connection = null;

        try {
            connection = outputSchema.getConnection();
            JdbcQueryUtils.dropTemporaryOutputTables(tmpOutputTable,
                    outputNumberOfPartitions, connection);

        } catch (SQLException ex1) {
            LOG.error(ex1.getMessage());
            throw new RetryException(ex1.getMessage());
        } catch (ClassNotFoundException ex2) {
            LOG.error(ex2.getMessage());
            throw new UnrecoverableException(ex2.getMessage());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }
}
