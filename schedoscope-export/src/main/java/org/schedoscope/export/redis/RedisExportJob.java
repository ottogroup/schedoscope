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

package org.schedoscope.export.redis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.redis.outputformat.RedisHashWritable;
import org.schedoscope.export.redis.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.RedisMRJedisFactory;

import redis.clients.jedis.Jedis;

/**
 * The MR driver to run the Hive to Redis export. Depending on the cmdl params
 * it either runs a full table export or only a selected field.
 */
public class RedisExportJob extends Configured implements Tool {

	@Option(name = "-s", usage = "set to true if kerberos is enabled")
	private boolean isSecured = false;

	@Option(name = "-m", usage = "specify the metastore URIs", required = true)
	private String metaStoreUris;

	@Option(name = "-p", usage = "the kerberos principal", depends = { "-s" })
	private String principal;

	@Option(name = "-h", usage = "redis host")
	private String redisHost = "localhost";

	@Option(name = "-P", usage = "redis port")
	private int redisPort = 6379;

	@Option(name = "-K", usage = "redis key space (default is 0)")
	private int redisDb = 0;

	@Option(name = "-d", usage = "input database", required = true)
	private String inputDatabase;

	@Option(name = "-t", usage = "input table", required = true)
	private String inputTable;

	@Option(name = "-i", usage = "input filter, e.g. \"month='08' and year='2015'\"")
	private String inputFilter;

	@Option(name = "-k", usage = "key column", required = true)
	private String keyName;

	@Option(name = "-v", usage = "value column, if empty full table export", depends = { "-k" })
	private String valueName;

	@Option(name = "-r", usage = "optional key prefix for redis key")
	private String keyPrefix = "";

	@Option(name = "-c", usage = "number of reducers, concurrency level")
	private int numReducer = 2;

	@Option(name = "-a", usage = "append data to existing keys, only useful for native export of map/list types")
	boolean replace = false;

	@Option(name = "-l", usage = "pipeline mode for redis client")
	boolean pipeline = false;

	@Option(name = "-f", usage = "flush redis key space")
	boolean flush = false;

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
		return (success ? 0 : 1);
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
	 * @param redisHost
	 *            The Redis host.
	 * @param redisPort
	 *            The Redis port.
	 * @param redisDb
	 *            The Redis key space / database.
	 * @param inputDatabase
	 *            The Hive input database
	 * @param inputTable
	 *            The Hive inut table.
	 * @param inputFilter
	 *            An optional filter for Hive.
	 * @param keyName
	 *            The field name to use as key.
	 * @param valueName
	 *            The fields name to use a value, can be null.
	 * @param keyPrefix
	 *            An optional key prefix.
	 * @param numReducer
	 *            Number of reducers / partitions.
	 * @param replace
	 *            A flag indicating of data should be replaced.
	 * @param pipeline
	 *            A flag to set the Redis client pipeline mode.
	 * @param flush
	 *            A flag indicating Redis key space should be flushed.
	 * @return A configured job instance
	 * @throws Exception
	 *             is thrown if an error occurs.
	 */
	public Job configure(boolean isSecured, String metaStoreUris,
			String principal, String redisHost, int redisPort, int redisDb,
			String inputDatabase, String inputTable, String inputFilter,
			String keyName, String valueName, String keyPrefix, int numReducer,
			boolean replace, boolean pipeline, boolean flush) throws Exception {

		this.isSecured = isSecured;
		this.metaStoreUris = metaStoreUris;
		this.principal = principal;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.redisDb = redisDb;
		this.inputDatabase = inputDatabase;
		this.inputTable = inputTable;
		this.inputFilter = inputFilter;
		this.keyName = keyName;
		this.keyPrefix = keyPrefix;
		this.valueName = valueName;
		this.numReducer = numReducer;
		this.replace = replace;
		this.pipeline = pipeline;
		this.flush = flush;
		return configure();
	}

	private Job configure() throws Exception {

		Configuration conf = getConf();

		if (metaStoreUris.startsWith("thrift://")) {
			conf.set("hive.metastore.local", "false");
			conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUris);
		} else {
			conf.set("hive.metastore.local", "true");
			conf.unset(HiveConf.ConfVars.METASTOREURIS.varname);
			conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
					metaStoreUris);
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

		Job job = Job.getInstance(conf, "RedisExport: " + inputDatabase + "."
				+ inputTable);

		job.setJarByClass(RedisExportJob.class);

		if (inputFilter == null || inputFilter.trim().equals("")) {
			HCatInputFormat.setInput(job, inputDatabase, inputTable);

		} else {
			HCatInputFormat.setInput(job, inputDatabase, inputTable,
					inputFilter);
		}

		HCatSchema hcatSchema = HCatInputFormat.getTableSchema(job
				.getConfiguration());

		Class<?> OutputClazz;

		if (valueName == null) {
			RedisOutputFormat.setOutput(job.getConfiguration(), redisHost,
					redisPort, redisDb, keyName, keyPrefix, replace, pipeline);

			job.setMapperClass(RedisFullTableExportMapper.class);
			OutputClazz = RedisHashWritable.class;

		} else {
			RedisOutputFormat.setOutput(job.getConfiguration(), redisHost,
					redisPort, redisDb, keyName, keyPrefix, valueName, replace,
					pipeline);
			job.setMapperClass(RedisExportMapper.class);
			OutputClazz = RedisOutputFormat.getRedisWritableClazz(hcatSchema,
					valueName);
		}

		if (flush) {
			Jedis jedis = RedisMRJedisFactory.getJedisClient(job
					.getConfiguration());
			jedis.flushDB();
		}

		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(numReducer);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputClazz);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutputClazz);
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
			int exitCode = ToolRunner.run(new RedisExportJob(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
