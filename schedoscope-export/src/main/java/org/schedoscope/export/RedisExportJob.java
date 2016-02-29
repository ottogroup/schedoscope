package org.schedoscope.export;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.outputformat.RedisHashWritable;
import org.schedoscope.export.outputformat.RedisOutputFormat;

public class RedisExportJob extends Configured implements Tool {

	@Option(name="-s", usage="set to true if kerberos is enabled")
	private boolean isSecured = false;

	@Option(name="-m", usage="specify the metastore URI")
	private String metaStoreUris;

	@Option(name="-p", usage="the kerberos principal", depends={"-s"})
	private String principal;

	@Option(name="-h", usage="redis host")
	private String redisHost;

	@Option(name="-d", usage="input database", required=true)
	private String inputDatabase;

	@Option(name="-t", usage="input table", required=true)
	private String inputTable;

	@Option(name="-k", usage="key column", required=true)
	private String keyName;

	@Option(name="-v", usage="value column, if empty full table export", depends={"-k"})
	private String valueName;

	@Option(name="-r", usage="optional key prefix for redis key")
	private String keyPrefix = "";

	@Option(name="-c", usage="number of reducers, concurrency level")
	private int numReducer = 2;

	@Option(name="-a", usage="append data to existing keys, only useful for native export of map/list types")
	boolean append = false;

	@Option(name="-l", usage="pipeline mode for redis client")
	boolean pipeline = false;

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		CmdLineParser cmd = new CmdLineParser(this);

		try {
			cmd.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			cmd.printUsage(System.err);
			System.exit(1);
		}

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

		Job job = Job.getInstance(conf, "RedisExport");

		HCatInputFormat.setInput(conf, inputDatabase, inputTable);
		HCatSchema hCatSchema = HCatInputFormat.getTableSchema(job.getConfiguration());

		Class<?> OutputClazz = RedisHashWritable.class;

		if (valueName.isEmpty()) {
			job.setMapperClass(RedisFullTableExportMapper.class);
			RedisOutputFormat.setOutput(conf, redisHost, keyName, keyPrefix, append, pipeline);
		} else {
			job.setMapperClass(RedisExportMapper.class);
			RedisOutputFormat.setOutput(conf, redisHost, keyName, keyPrefix, valueName, append, pipeline);
			OutputClazz = RedisOutputFormat.getRedisWritableClazz(hCatSchema, valueName);
		}

		job.setReducerClass(RedisExportReducer.class);
		job.setNumReduceTasks(numReducer);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputClazz);
		job.setOutputKeyClass(OutputClazz);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);

		return (success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		try {
			int exitCode = ToolRunner.run(new RedisExportJob(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
