package org.schedoscope.export.outputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;
import org.schedoscope.export.utils.JdbcQueryUtils;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class JdbcOutputFormat<K extends DBWritable, V> extends
		OutputFormat<K, V> {

	private static final Log LOG = LogFactory.getLog(JdbcOutputFormat.class);
	private static final String TMPDB = "TMP_";

	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
	}

	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
				context);
	}

	@InterfaceStability.Evolving
	public class JdbcRecordWriter extends RecordWriter<K, V> {

		private Connection connection;
		private PreparedStatement statement;
		private int rowsInBatch = 0;
		private int commitSize = 25000;

		public JdbcRecordWriter() throws SQLException {
		}

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

		public void close(TaskAttemptContext context) throws IOException {
			try {
				statement.executeBatch();
				connection.commit();
			} catch (SQLException e) {
				try {
					connection.rollback();
				} catch (SQLException ex) {
					LOG.warn(StringUtils.stringifyException(ex));
				}
				throw new IOException(e.getMessage());
			} finally {
				try {
					statement.close();
					connection.close();
				} catch (SQLException ex) {
					throw new IOException(ex.getMessage());
				}
			}
		}

		public void write(K key, V value) throws IOException {
			try {
				key.write(statement);
				statement.addBatch();
				if (rowsInBatch == commitSize) {
					statement.executeBatch();
					rowsInBatch = 0;
				} else {
					rowsInBatch++;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException {

		Schema outputSchema = SchemaFactory.getSchema(context
				.getConfiguration());

		String tmpOutputTable = TMPDB + outputSchema.getTable() + "_"
				+ context.getTaskAttemptID().getTaskID().getId();
		String createTableQuery = outputSchema.getCreateTableQuery();
		createTableQuery = createTableQuery.replace(outputSchema.getTable(),
				tmpOutputTable);
		int commitSize = outputSchema.getCommitSize();
		String[] fieldNames = outputSchema.getColumnNames();

		try {
			Connection connection = outputSchema.getConnection();

			JdbcQueryUtils.dropTemporaryOutputTable(tmpOutputTable, connection);
			JdbcQueryUtils.createTable(createTableQuery, connection);

			PreparedStatement statement = null;

			statement = connection.prepareStatement(JdbcQueryUtils
					.createInsertQuery(tmpOutputTable, fieldNames));

			return new JdbcRecordWriter(connection, statement, commitSize);
		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}

	public static void setOutput(Configuration conf, String driver,
			String connectionString, String username, String password,
			String outputTable, String inputFilter,
			int outputNumberOfPartitions, int outputCommitSize,
			String[] columnNames, String[] columnsTypes) throws IOException {
		Schema outputSchema = SchemaFactory.getSchema(driver, conf);
		outputSchema.setOutput(driver, connectionString, username, password,
				outputTable, inputFilter, outputNumberOfPartitions,
				outputCommitSize, columnNames, columnsTypes);

	}

	public static void finalizeOutput(Configuration conf) throws IOException {
		Schema outputSchema = SchemaFactory.getSchema(conf);
		String outputTable = outputSchema.getTable();
		String tmpOutputTable = TMPDB + outputSchema.getTable();
		String createTableStatement = outputSchema.getCreateTableQuery();
		String inputFilter = outputSchema.getFilter();
		int outputNumberOfPartitions = outputSchema.getNumberOfPartitions();

		try {
			Connection connection = outputSchema.getConnection();
			if (inputFilter != null) {
				JdbcQueryUtils.deleteExisitingRows(outputTable, inputFilter,
						connection);
			} else {
				JdbcQueryUtils
						.dropTemporaryOutputTable(outputTable, connection);
			}
			JdbcQueryUtils.createTable(createTableStatement, connection);
			JdbcQueryUtils.mergeOutput(outputTable, outputNumberOfPartitions,
					connection);
			JdbcQueryUtils.dropTemporaryOutputTables(tmpOutputTable,
					outputNumberOfPartitions, connection);

		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		} finally {
			try {
				outputSchema.getConnection().commit();
				outputSchema.getConnection().close();
			} catch (ClassNotFoundException e) {
			} catch (SQLException e) {
			}

		}
	}

	public static void rollback(Configuration conf) throws IOException {
		Schema outputSchema = SchemaFactory.getSchema(conf);
		String tmpOutputTable = TMPDB + outputSchema.getTable();
		int outputNumberOfPartitions = outputSchema.getNumberOfPartitions();

		try {
			Connection connection = outputSchema.getConnection();
			JdbcQueryUtils.dropTemporaryOutputTables(tmpOutputTable,
					outputNumberOfPartitions, connection);

		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		} finally {
			try {
				outputSchema.getConnection().commit();
				outputSchema.getConnection().close();
			} catch (ClassNotFoundException e) {
			} catch (SQLException e) {
			}

		}
	}

}
