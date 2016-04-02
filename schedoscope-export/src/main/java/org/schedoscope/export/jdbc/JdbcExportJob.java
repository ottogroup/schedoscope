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

package org.schedoscope.export.jdbc;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputFormat;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputWritable;
import org.schedoscope.export.jdbc.outputschema.Schema;
import org.schedoscope.export.jdbc.outputschema.SchemaFactory;
import org.schedoscope.export.jdbc.outputschema.SchemaUtils;

/**
 * The MR driver to run the Hive to database export, uses JDBC under the hood.
 */
public class JdbcExportJob extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(JdbcExportJob.class);

	private static final String LOCAL_PATH_PREFIX = "file://";

	@Option(name = "-s", usage = "set to true if kerberos is enabled")
	private boolean isSecured = false;

	@Option(name = "-m", usage = "specify the metastore URI")
	private String metaStoreUris;

	@Option(name = "-p", usage = "the kerberos principal", depends = { "-s" })
	private String principal;

	@Option(name = "-j", usage = "the jdbc connection string, jdbc:mysql://remote-host:3306/schema", required = true)
	private String dbConnectionString;

	@Option(name = "-u", usage = "the database user")
	private String dbUser;

	@Option(name = "-w", usage = "the database password", depends = { "-u" })
	private String dbPassword;

	@Option(name = "-d", usage = "input database", required = true)
	private String inputDatabase;

	@Option(name = "-t", usage = "input table", required = true)
	private String inputTable;

	@Option(name = "-i", usage = "input filter, e.g. \"month='08' and year='2015'\"")
	private String inputFilter;

	@Option(name = "-e", usage = "storage engine, either 'InnoDB' or 'MyISAM', works only for MySQL")
	private String storageEngine;

	@Option(name = "-x", usage = "columns to use for the 'DISTRIBUTE BY' clause, only Exasol")
	private String distributeBy;

	@Option(name = "-c", usage = "number of reducers, concurrency level")
	private int numReducer = 2;

	@Option(name = "-k", usage = "batch size")
	private int commitSize = 10000;

	@Override
	public int run(String[] args) throws Exception {

		CmdLineParser cmd = new CmdLineParser(this);

		try {
			cmd.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			cmd.printUsage(System.err);
			throw e;
		}

		Job job = configure();
		boolean success = job.waitForCompletion(true);

		postCommit(success, job.getConfiguration());
		return (success ? 0 : 1);
	}

	/**
	 * Triggers the post commit action, in this instance the final table is
	 * created and filled with data from all partition tables.
	 *
	 * @param jobSuccessful
	 *            A flag indicating if job was successful.
	 * @param conf
	 *            The Hadoop configuration object.
	 * @throws IOException
	 *             Is thrown if an error occurs.
	 */
	public void postCommit(boolean jobSuccessful, Configuration conf)
			throws IOException {

		if (jobSuccessful) {
			JdbcOutputFormat.finalizeOutput(conf);
		} else {
			JdbcOutputFormat.rollback(conf);
		}
	}

	/**
	 * This function takes all required parameters and returns a configured job
	 * object.
	 *
	 * @param isSecured
	 *            A flag indicating if Kerberos is enabled.
	 * @param metaStoreUris
	 *            A string containing the Hive meta store URI
	 * @param principal
	 *            The Kerberos principal.
	 * @param dbConnectionString
	 *            The JDBC connection string.
	 * @param dbUser
	 *            The database user
	 * @param dbPassword
	 *            The database password
	 * @param inputDatabase
	 *            The Hive input database
	 * @param inputTable
	 *            The Hive input table
	 * @param inputFilter
	 *            An optional input filter.
	 * @param storageEngine
	 *            An optional storage engine (only MySQL)
	 * @param distributeBy
	 *            An optional distribute by clause (only Exasol)
	 * @param numReducer
	 *            Number of reducers / partitions
	 * @param commitSize
	 *            The batch size.
	 * @return A configured job instance.
	 * @throws Exception
	 *             Is thrown if an error occurs.
	 */
	public Job configure(boolean isSecured, String metaStoreUris,
			String principal, String dbConnectionString, String dbUser,
			String dbPassword, String inputDatabase, String inputTable,
			String inputFilter, String storageEngine, String distributeBy,
			int numReducer, int commitSize) throws Exception {

		this.isSecured = isSecured;
		this.metaStoreUris = metaStoreUris;
		this.principal = principal;
		this.dbConnectionString = dbConnectionString;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.inputDatabase = inputDatabase;
		this.inputTable = inputTable;
		this.inputFilter = inputFilter;
		this.storageEngine = storageEngine;
		this.distributeBy = distributeBy;
		this.numReducer = numReducer;
		this.commitSize = commitSize;
		return configure();
	}

	private Job configure() throws Exception {

		Configuration conf = getConf();

		conf.set("hive.metastore.local", "false");
		conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUris);

		if (isSecured) {
			conf.setBoolean(
					HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
			conf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
					principal);

			if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
				conf.set("mapreduce.job.credentials.binary",
						System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			}
		}

		Job job = Job.getInstance(conf, "JDBCExport: " + inputDatabase + "."
				+ inputTable);

		job.setJarByClass(JdbcExportJob.class);
		job.setMapperClass(JdbcExportMapper.class);
		job.setReducerClass(JdbcExportReducer.class);
		job.setNumReduceTasks(numReducer);

		if (inputFilter == null || inputFilter.trim().equals("")) {
			HCatInputFormat.setInput(job, inputDatabase, inputTable);

		} else {
			HCatInputFormat.setInput(job, inputDatabase, inputTable,
					inputFilter);
		}

		Schema outputSchema = SchemaFactory.getSchema(dbConnectionString,
				job.getConfiguration());
		HCatSchema hcatInputSchema = HCatInputFormat.getTableSchema(job
				.getConfiguration());

		String[] columnNames = SchemaUtils.getColumnNamesFromHcatSchema(
				hcatInputSchema, outputSchema);
		String[] columnTypes = SchemaUtils.getColumnTypesFromHcatSchema(
				hcatInputSchema, outputSchema);

		String outputTable = inputDatabase + "_" + inputTable;

		JdbcOutputFormat.setOutput(job.getConfiguration(), dbConnectionString,
				dbUser, dbPassword, outputTable, inputFilter, numReducer,
				commitSize, storageEngine, distributeBy, columnNames,
				columnTypes);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(JdbcOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JdbcOutputWritable.class);
		job.setOutputKeyClass(JdbcOutputWritable.class);
		job.setOutputValueClass(NullWritable.class);

		Class<?> clazz = Class.forName(outputSchema.getDriverName());
		String jarFile = ClassUtil.findContainingJar(clazz);
		String jarSelf = ClassUtil.findContainingJar(JdbcExportJob.class);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		String tmpDir = job.getConfiguration().get("hadoop.tmp.dir");
		Path hdfsDir = new Path(tmpDir + "/" + new Path(jarFile).getName());

		if (jarFile != null && jarSelf != null && tmpDir != null
				&& !jarFile.equals(jarSelf)) {
			LOG.info("copy " + LOCAL_PATH_PREFIX + jarFile + " to " + tmpDir);
			fs.copyFromLocalFile(false, true, new Path(LOCAL_PATH_PREFIX
					+ jarFile), hdfsDir);
			LOG.info("add " + tmpDir + "/" + jarFile + " to distributed cache");
			job.addArchiveToClassPath(hdfsDir);
		}

		return job;
	}

	/**
	 * The entry point when called from the command line.
	 *
	 * @param args
	 *            A string array containing the cmdl args.
	 * @throws Exception
	 *             is thrown if an error occurs.
	 */
	public static void main(String[] args) throws Exception {
		try {
			int exitCode = ToolRunner.run(new JdbcExportJob(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
	}
}