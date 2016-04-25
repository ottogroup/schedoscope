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

package org.schedoscope.export.kafka;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.kafka.avro.HCatToAvroSchemaConverter;
import org.schedoscope.export.kafka.options.CleanupPolicy;
import org.schedoscope.export.kafka.options.CompressionCodec;
import org.schedoscope.export.kafka.options.OutputEncoding;
import org.schedoscope.export.kafka.options.ProducerType;
import org.schedoscope.export.kafka.outputformat.KafkaOutputFormat;

import com.google.common.collect.ImmutableSet;

/**
 * The MR driver to run the Hive to Kafka export. Depending on the cmdl params
 * it either runs in regular mode or in log compaction mode.
 */
public class KafkaExportJob extends BaseExportJob {

	private static final Log LOG = LogFactory.getLog(KafkaExportJob.class);

	@Option(name = "-s", usage = "set to true if kerberos is enabled")
	private boolean isSecured = false;

	@Option(name = "-m", usage = "specify the metastore URI")
	private String metaStoreUris;

	@Option(name = "-p", usage = "the kerberos principal", depends = { "-s" })
	private String principal;

	@Option(name = "-d", usage = "input database", required = true)
	private String inputDatabase;

	@Option(name = "-t", usage = "input table", required = true)
	private String inputTable;

	@Option(name = "-i", usage = "input filter, e.g. \"month='08' and year='2015'\"")
	private String inputFilter;

	@Option(name = "-k", usage = "key column")
	private String keyName;

	@Option(name = "-b", usage = "list of brokers: host1:9092,host2:9092,host3:9092", required = true)
	private String brokerList;

	@Option(name = "-z", usage = "list of zookeeper hosts: host1:2181,host2:2181,host3:2181", required = true)
	private String zookeeperHosts;

	@Option(name = "-P", usage = "broker type, either 'async' or 'sync'")
	private ProducerType producerType = ProducerType.sync;

	@Option(name = "-w", usage = "cleanup policy, either 'delete' or 'compact'")
	private CleanupPolicy cleanupPolicy = CleanupPolicy.delete;

	@Option(name = "-n", usage = "number of partitons, default to 1")
	private int numPartitions = 1;

	@Option(name = "-r", usage = "replication factor, defaults to 1")
	private int replicationFactor = 1;

	@Option(name = "-c", usage = "number of reducers, concurrency level")
	private int numReducer = 2;

	@Option(name = "-x", usage = "compression codec, either 'none', 'snappy' or 'gzip'")
	private CompressionCodec codec = CompressionCodec.none;

	@Option(name = "-o", usage = "output encoding, either 'string' or 'avro'")
	private OutputEncoding encoding = OutputEncoding.string;

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
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 *
	 * @param isSecured
	 *            A flag indicating if Kerberos is enabled
	 * @param metaStoreUris
	 *            The Hive metastore uri(s)
	 * @param principal
	 *            The Kerberos principal
	 * @param inputDatabase
	 *            The Hive input database
	 * @param inputTable
	 *            Hive input table
	 * @param inputFilter
	 *            An optional filter
	 * @param keyName
	 *            The name of the database column used as key
	 * @param brokers
	 *            A list of Kafka brokers
	 * @param zookeepers
	 *            A list of zookeeper brokers
	 * @param producerType
	 *            The Kafka producer type (sync / async)
	 * @param cleanupPolicy
	 *            The cleanup policy (delete / compact)
	 * @param numPartitions
	 *            Num of partitions for the Kafka topic
	 * @param replicationFactor
	 *            The replication factor for the topic
	 * @param numReducer
	 *            The number of reducers
	 * @param codec
	 *            The compression codec (gzip / snappy / none)
	 * @param outputEncoding
	 *            Output encoding (string / avro)
	 * @param anonFields
	 *            A list of fields to anonymize
	 * @param exportSalt
	 *            An optional salt when anonymizing fields
	 * @return A configured Job instance
	 * @throws Exception
	 *             Is thrown if an error occurs
	 */
	public Job configure(boolean isSecured, String metaStoreUris, String principal, String inputDatabase,
			String inputTable, String inputFilter, String keyName, String brokers, String zookeepers,
			ProducerType producerType, CleanupPolicy cleanupPolicy, int numPartitions, int replicationFactor,
			int numReducer, CompressionCodec codec, OutputEncoding outputEncoding, String[] anonFields,
			String exportSalt) throws Exception {

		this.isSecured = isSecured;
		this.metaStoreUris = metaStoreUris;
		this.principal = principal;
		this.inputDatabase = inputDatabase;
		this.inputTable = inputTable;
		this.inputFilter = inputFilter;
		this.keyName = keyName;
		this.brokerList = brokers;
		this.zookeeperHosts = zookeepers;
		this.producerType = producerType;
		this.cleanupPolicy = cleanupPolicy;
		this.numPartitions = numPartitions;
		this.replicationFactor = replicationFactor;
		this.numReducer = numReducer;
		this.codec = codec;
		this.encoding = outputEncoding;
		this.anonFields = anonFields.clone();
		this.exportSalt = exportSalt;
		return configure();
	}

	private Job configure() throws Exception {

		Configuration conf = getConfiguration();
		conf = configureHiveMetaStore(conf, metaStoreUris);
		conf = configureKerberos(conf, isSecured, principal);

		Job job = Job.getInstance(conf, "KafkaExport: " + inputDatabase + "." + inputTable);

		job.setJarByClass(KafkaExportJob.class);

		if (inputFilter == null || inputFilter.trim().equals("")) {
			HCatInputFormat.setInput(job, inputDatabase, inputTable);

		} else {
			HCatInputFormat.setInput(job, inputDatabase, inputTable, inputFilter);
		}

		HCatSchema hcatSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
		HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter(ImmutableSet.copyOf(anonFields));
		Schema avroSchema = schemaConverter.convertSchema(hcatSchema, inputTable);
		AvroJob.setMapOutputValueSchema(job, avroSchema);

		KafkaOutputFormat.setOutput(job.getConfiguration(), brokerList, zookeeperHosts, producerType, cleanupPolicy,
				keyName, inputTable, inputDatabase, numPartitions, replicationFactor, codec, encoding);

		job.setMapperClass(KafkaExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(numReducer);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(KafkaOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AvroValue.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvroValue.class);

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
			int exitCode = ToolRunner.run(new KafkaExportJob(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
	}
}
