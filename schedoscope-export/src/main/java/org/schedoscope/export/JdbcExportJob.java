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
import org.schedoscope.export.outputformat.JdbcOutputFormat;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;
import org.schedoscope.export.outputschema.SchemaUtils;

public class JdbcExportJob extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(JdbcExportJob.class);

	private boolean isSecured;
	private String metastoreuris;
	private String principal;
	private String dbDriver;
	private String dbConnectionString;
	private String dbUser;
	private String dbPassword;
	private String inputDatabase;
	private String inputTable;
	private String outputTable;
	private String inputFilter;
	private int outputNumberOfPartitions;
	private int outputCommitSize;
	private String jobName;

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		parseArguments(args);

		conf.set("hive.metastore.local", "false");
		conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metastoreuris);

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

		Schema outputSchema = SchemaFactory.getSchema(dbDriver,
				job.getConfiguration());
		HCatSchema hcatInputSchema = HCatInputFormat.getTableSchema(job
				.getConfiguration());

		String[] columnNames = SchemaUtils.getColumnNamesFromHcatInputSchema(
				hcatInputSchema, outputSchema.getColumnNameMapping());
		String[] columnTypes = SchemaUtils.getColumnTypesFromHcatInputSchema(
				hcatInputSchema, outputSchema.getColumnTypeMapping());

		JdbcOutputFormat.setOutput(job.getConfiguration(), dbDriver,
				dbConnectionString, dbUser, dbPassword, outputTable,
				inputFilter, outputNumberOfPartitions, outputCommitSize,
				columnNames, columnTypes);

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

		int exitCode = ToolRunner.run(new JdbcExportJob(), args);
		System.exit(exitCode);
	}

	private void parseArguments(String[] args) {

		for (String arg : args) {
			LOG.info("arg: " + arg);
		}

		isSecured = Boolean.parseBoolean(args[0]);
		if (isSecured && args.length == 13) {

			metastoreuris = args[1];
			principal = args[2];
			dbDriver = args[3];
			dbConnectionString = args[4];
			dbUser = args[5];
			dbPassword = args[6];
			inputDatabase = args[7];
			inputTable = args[8];
			outputTable = args[7] + "_" + args[8];
			inputFilter = args[9];
			outputNumberOfPartitions = Integer.valueOf(args[10]);
			outputCommitSize = Integer.valueOf(args[11]);
			jobName = args[12];

		} else if (isSecured && args.length == 12) {

			metastoreuris = args[1];
			principal = args[2];
			dbDriver = args[3];
			dbConnectionString = args[4];
			dbUser = args[5];
			dbPassword = args[6];
			inputDatabase = args[7];
			inputTable = args[8];
			outputTable = args[7] + "_" + args[8];
			outputNumberOfPartitions = Integer.valueOf(args[9]);
			outputCommitSize = Integer.valueOf(args[10]);
			jobName = args[11];

		} else if (!isSecured && args.length == 12) {

			metastoreuris = args[1];
			dbDriver = args[2];
			dbConnectionString = args[3];
			dbUser = args[4];
			dbPassword = args[5];
			inputDatabase = args[6];
			inputTable = args[7];
			outputTable = args[6] + "_" + args[7];
			inputFilter = args[8];
			outputNumberOfPartitions = Integer.valueOf(args[9]);
			outputCommitSize = Integer.valueOf(args[10]);
			jobName = args[11];

		} else if (!isSecured && args.length == 11) {

			metastoreuris = args[1];
			dbDriver = args[2];
			dbConnectionString = args[3];
			dbUser = args[4];
			dbPassword = args[5];
			inputDatabase = args[6];
			inputTable = args[7];
			outputTable = args[6] + "_" + args[7];
			outputNumberOfPartitions = Integer.valueOf(args[8]);
			outputCommitSize = Integer.valueOf(args[9]);
			jobName = args[10];

		} else {
			throw new IllegalArgumentException("Illegal number of arguments");
		}
	}

}