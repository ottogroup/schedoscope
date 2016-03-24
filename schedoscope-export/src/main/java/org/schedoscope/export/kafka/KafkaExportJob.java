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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.kafka.avro.HCatToAvroRecordConverter;
import org.schedoscope.export.kafka.outputformat.CleanupPolicy;
import org.schedoscope.export.kafka.outputformat.CompressionCodec;
import org.schedoscope.export.kafka.outputformat.KafkaOutputFormat;
import org.schedoscope.export.kafka.outputformat.ProducerType;

/**
 * The MR driver to run the Hive to Kafka export. Depending on the
 * cmdl params it either runs in regular mode or in log compaction
 * mode.
 *
 * TODO: assertions in unit test + add more unit tests
 */
public class KafkaExportJob extends Configured implements Tool {

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

    @Option(name = "-c", usage = "cleanup policy, either 'delete' or 'compact'")
    private CleanupPolicy cleanupPolicy = CleanupPolicy.delete;

    @Option(name = "-n", usage = "number of partitons, default to 1")
    private int numPartitions = 1;

    @Option(name = "-r", usage = "replication factor, defaults to 1")
    private int replicationFactor = 1;

    @Option(name = "-x", usage = "compression codec, either 'none', 'snappy' or 'gzip'")
    private CompressionCodec codec = CompressionCodec.none;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        conf.set("hive.metastore.local", "false");
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUris);

        if (isSecured) {
            conf.setBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
            conf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, principal);

            if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
                conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            }
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(KafkaExportJob.class);

        if (inputFilter == null || inputFilter.trim().equals("")) {
            HCatInputFormat.setInput(job, inputDatabase, inputTable);

        } else {
            HCatInputFormat.setInput(job, inputDatabase, inputTable, inputFilter);
        }

        HCatSchema hcatSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
        Schema avroSchema = HCatToAvroRecordConverter.convertSchema(hcatSchema, inputTable);
        AvroJob.setMapOutputValueSchema(job, avroSchema);

        KafkaOutputFormat.setOutput(job.getConfiguration(), brokerList, zookeeperHosts, producerType,
                cleanupPolicy, keyName, inputTable, numPartitions, replicationFactor, codec);

        job.setMapperClass(KafkaExportMapper.class);
        job.setReducerClass(KafkaExportReducer.class);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(KafkaOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvroValue.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new KafkaExportJob(), args);
        System.exit(exitCode);
    }
}
