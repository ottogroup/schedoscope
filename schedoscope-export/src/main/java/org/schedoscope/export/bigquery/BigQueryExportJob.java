package org.schedoscope.export.bigquery;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.jdbc.JdbcExportJob;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hive.hcatalog.common.HCatUtil.getTable;
import static org.apache.hive.hcatalog.common.HCatUtil.getTableSchemaWithPtnCols;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.configureBigQueryOutputFormat;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputFormat.*;

public class BigQueryExportJob extends BaseExportJob {

    private static final Log LOG = LogFactory.getLog(BigQueryExportJob.class);

    @Option(name = "-P", usage = "the GCP project ID under which to create the resulting BigQuery dataset, e.g., project 4711. If not passed, the default GCP project will be used")
    private String project;

    @Option(name = "-D", usage = "the BigQuery table partition date into which to insert the exported data, e.g., 20171001. If not passed, it is assumed that the resulting BigQuery table is not partitioned")
    private String partitionDate;

    @Option(name = "-x", usage = "the postfix to append to the resulting BigQuery table name, e.g., EC0101. If not passed, no postfix will be appended")
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


    @Override
    public int run(String[] args) throws CmdLineException, IOException, TException, ClassNotFoundException, InterruptedException, TimeoutException {

        CmdLineParser cmd = new CmdLineParser(this);

        try {
            cmd.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmd.printUsage(System.err);
            throw e;
        }

        Configuration conf = prepareConfiguration();
        Job job = prepareJob(conf);

        boolean success = job.waitForCompletion(true);

        if (success) {
            try {
                prepareBigQueryTable(conf);
                commit(conf);
            } catch (Throwable t) {
                rollback(conf);
            }
        } else
            rollback(conf);

        return (success ? 0 : 1);
    }

    private Configuration prepareConfiguration() throws IOException, TException {
        Configuration conf = getConfiguration();
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

        return configureBigQueryOutputFormat(
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

    }

    private Job prepareJob(Configuration conf) throws IOException, TException {

        Job job = Job.getInstance(conf, "BigQueryExport: " + inputDatabase + "."
                + inputTable);

        return job;
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new JdbcExportJob(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }
    }
}
