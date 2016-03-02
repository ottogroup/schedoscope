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
package org.schedoscope.export;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.outputformat.JdbcOutputFormat;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;
import org.schedoscope.export.outputschema.SchemaUtils;

public class JdbcExportJob extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(JdbcExportJob.class);

	@Option(name="-s", usage="set to true if kerberos is enabled")
	private boolean isSecured = false;

	@Option(name="-m", usage="specify the metastore URI")
	private String metaStoreUris;

	@Option(name="-p", usage="the kerberos principal", depends={"-s"})
	private String principal;

	@Option(name="-j", usage="the jdbc connection string, jdbc:mysql://remote-host:3306/schema", required=true)
	private String dbConnectionString;

	@Option(name="-u", usage="the database user")
	private String dbUser;

	@Option(name="-w", usage="the database password")
	private String dbPassword;

	@Option(name="-d", usage="input database", required=true)
	private String inputDatabase;

	@Option(name="-t", usage="input table", required=true)
	private String inputTable;

	@Option(name="-i", usage="input filter, e.g. \"month='08' and year='2015'\"")
	private String inputFilter;

	@Option(name="-e", usage="storage engine, either 'InnoDB' or 'MyISAM', works only for MySQL")
	private String storageEngine;

	@Option(name="-x", usage="columns to use for the 'DISTRIBUTED BY' clause, only Exasol")
	private String distributedBy;

	@Option(name="-c", usage="number of reducers, concurrency level")
	private int outputNumberOfPartitions = 2;

	@Option(name="-k", usage="batch size")
	private int outputCommitSize = 10000;

	@Option(name="-q", usage="job name")
	private String jobName;

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		CmdLineParser cmd = new CmdLineParser(this);

		try {
			cmd.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			cmd.printUsage(System.err);
			System.exit(1);
		}

		String outputTable = inputDatabase + "_" + inputTable;

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

		Job job = Job.getInstance(conf, jobName);

		job.setJarByClass(JdbcExportJob.class);
		job.setMapperClass(JdbcExportMapper.class);
		job.setReducerClass(JdbcExportReducer.class);
		job.setNumReduceTasks(outputNumberOfPartitions);

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

		JdbcOutputFormat.setOutput(job.getConfiguration(),
				dbConnectionString, dbUser, dbPassword, outputTable,
				inputFilter, outputNumberOfPartitions, outputCommitSize,
				storageEngine, distributedBy, columnNames, columnTypes);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(JdbcOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(JdbcOutputWritable.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);

		if (success) {
			JdbcOutputFormat.finalizeOutput(job.getConfiguration());
		} else {
			JdbcOutputFormat.rollback(job.getConfiguration());
		}

		return (success ? 0 : 1);
	}

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