/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.export.bigquery;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.bigquery.outputformat.BigQueryOutputFormat;

import java.io.File;
import java.io.IOException;

import static org.apache.hive.hcatalog.common.HCatUtil.getTable;
import static org.apache.hive.hcatalog.common.HCatUtil.getTableSchemaWithPtnCols;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.configureBigQueryOutputFormat;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputFormat.*;

public class BigQueryExportJob extends BaseExportJob {

    private static final Log LOG = LogFactory.getLog(BigQueryExportJob.class);

    @Option(name = "-P", usage = "the GCP project ID under which to create the resulting BigQuery dataset, e.g., project-4711. If not passed, the default GCP project will be used")
    private String project;

    @Option(name = "-D", usage = "the BigQuery table partition date into which to insert the exported data, e.g., 20171001. If not passed, it is assumed that the resulting BigQuery table is not partitioned")
    private String partitionDate;

    @Option(name = "-x", usage = "the postfix to append with an underscore to the resulting BigQuery table name, e.g., mypartitionpostfix. If not passed, no postfix will be appended")
    private String tablePostfix;

    @Option(name = "-l", usage = "the location where to store the resulting BigQuery table, e.g., US. If not passed, EU will be used")
    private String tableStorageLocation;

    @Option(name = "-k", usage = "GCP key to use for authentication in JSON format. If not passed, the gcloud default user will be used")
    private String gcpKey;

    @Option(name = "-K", usage = "file with the GCP key to use for authentication in JSON format. If not passed, the gcloud default user will be used")
    private String gcpKeyFile;

    @Option(name = "-b", usage = "GCP storage bucket to use for temporal storage, e.g., my-storage-bucket-for-export.", required = true)
    private String exportStorageBucket;

    @Option(name = "-f", usage = "GCP storage bucket folder prefix to prepend to temporal storage blobs, e.g., scratch")
    private String exportStoragePrefix;

    @Option(name = "-r", usage = "GCP storage bucket region to use, e.g., europe-west1. Defaults to europe-west3")
    private String exportStorageRegion;

    @Option(name = "-y", usage = "Proxy host to use for GCP access")
    private String proxyHost;

    @Option(name = "-Y", usage = "Proxy port to use for GCP access")
    private String proxyPort;


    private Configuration initialConfiguration;

    @Override
    public int run(String[] args) throws CmdLineException, IOException, TException, ClassNotFoundException, InterruptedException {

        Job job = createJob(args);

        boolean success = job.waitForCompletion(true);

        finishJob(job, success);

        return (success ? 0 : 1);
    }

    public Job createJob(String[] args) throws IOException, TException, CmdLineException {
        CmdLineParser cmd = new CmdLineParser(this);

        try {
            cmd.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmd.printUsage(System.err);
            throw e;
        }
        return prepareJobObject(prepareJobConfiguration());
    }

    public static void finishJob(Job job, boolean wasSuccessful) {
        Configuration conf = job.getConfiguration();

        if (wasSuccessful) {
            try {
                prepareBigQueryTable(conf);
                commit(conf);
            } catch (Throwable t) {
                rollback(conf);
            }
        } else
            rollback(conf);
    }


    private Configuration prepareJobConfiguration() throws IOException, TException {
        Configuration conf = initialConfiguration;
        conf = configureHiveMetaStore(conf);
        conf = configureKerberos(conf);
        conf = configureAnonFields(conf);

        if (gcpKeyFile != null) {
            gcpKey = FileUtils.readFileToString(new File(gcpKeyFile));
        }

        HCatSchema hCatSchema;

        HiveMetaStoreClient metastore = new HiveMetaStoreClient(new HiveConf(conf, HiveConf.class));
        try {
            hCatSchema = getTableSchemaWithPtnCols(getTable(metastore, inputDatabase, inputTable));
        } finally {
            metastore.close();
        }

        conf = configureBigQueryOutputFormat(
                conf,
                project,
                gcpKey,
                inputDatabase,
                inputTable,
                tablePostfix,
                tableStorageLocation,
                partitionDate,
                inputFilter,
                hCatSchema,
                exportStorageBucket,
                exportStoragePrefix,
                exportStorageRegion,
                numReducer,
                proxyHost,
                proxyPort
        );

        return conf;
    }

    private Job prepareJobObject(Configuration conf) throws IOException, TException {

        Job job = Job.getInstance(conf, "BigQueryExport: " + inputDatabase + "."
                + inputTable);

        job.setJarByClass(BigQueryExportJob.class);
        job.setMapperClass(BigQueryExportMapper.class);
        job.setReducerClass(Reducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);

        if (inputFilter == null || inputFilter.trim().equals("")) {
            HCatInputFormat.setInput(job, inputDatabase, inputTable);
        } else {
            HCatInputFormat.setInput(job, inputDatabase, inputTable, inputFilter);
        }

        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(BigQueryOutputFormat.class);

        job.setNumReduceTasks(numReducer);

        return job;
    }


    public BigQueryExportJob(Configuration initialConfiguration) {
        this.initialConfiguration = initialConfiguration;
    }

    public BigQueryExportJob() {
        this(new Configuration());
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new BigQueryExportJob(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }
    }
}
