/**
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope.export.ftp;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.ftp.outputformat.FileOutputType;
import org.schedoscope.export.ftp.outputformat.FtpUploadOutputFormat;
import org.schedoscope.export.ftp.upload.FileCompressionCodec;
import org.schedoscope.export.kafka.avro.HCatToAvroSchemaConverter;
import org.schedoscope.export.writables.TextPairArrayWritable;

/**
 * The MR driver to run the Hive to (S)FTP server export.
 */
public class FtpExportJob extends BaseExportJob {

    private static final Log LOG = LogFactory.getLog(FtpExportJob.class);

    @Option(name = "-k", usage = "ssh private key file")
    private String keyFile = "~/.ssh/id_rsa";

    @Option(name = "-u", usage = "the (s)ftp user", required = true)
    private String ftpUser;

    @Option(name = "-w", usage = "the ftp password / sftp passphrase")
    private String ftpPass;

    @Option(name = "-j", usage = "the (s)ftp endpoint, e.g. ftp://ftp.example.com:21/path/to/", required = true)
    private String ftpEndpoint;

    @Option(name = "-q", usage = "file prefix for files send to (s)ftp server, defaults to 'database'-'table'")
    private String filePrefix;

    @Option(name = "-l", usage = "delimiter for csv export, defaults to '\t'")
    private String delimiter = "\t";

    @Option(name = "-h", usage = "print header in exported CSV files, defaults to 'true'")
    private boolean printHeader = true;

    @Option(name = "-x", usage = "passive mode, only for ftp connections, defaults to 'true'")
    private boolean passiveMode = true;

    @Option(name = "-z", usage = "user dir is root, home dir on remote end is (s)ftp root dir defaults to 'true'")
    private boolean userIsRoot = true;

    @Option(name = "-g", usage = "clean up hdfs dir after export, defaults to 'true'")
    private boolean cleanHdfsDir = true;

    @Option(name = "-y", usage = "compression codec, either 'none', 'gzip' or 'bzip2', defaults to 'gzip'")
    private FileCompressionCodec codec = FileCompressionCodec.gzip;

    @Option(name = "-v", usage = "file output encoding, either 'csv' or 'json', defaults to 'csv'")
    private FileOutputType fileType = FileOutputType.csv;

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
     * @param isSecured     A flag indicating if Kerberos is enabled.
     * @param metaStoreUris A string containing the Hive meta store URI
     * @param principal     The Kerberos principal
     * @param inputDatabase The Hive input database
     * @param inputTable    The Hive input table
     * @param inputFilter   An optional input filter
     * @param numReducer    Number of reducers / partitions
     * @param anonFields    A list of fields to anonymize
     * @param exportSalt    An optional salt when anonymizing fields
     * @param keyFile       A private ssh key file
     * @param ftpUser       The (s)ftp user
     * @param ftpPass       The (s)ftp password or passphrase is key file is set
     * @param ftpEndpoint   The (s)ftp endpoint.
     * @param filePrefix    A custom file prefix for exported files
     * @param delimiter     A custom delimiter to use
     * @param printHeader   To print a header or not (only CSV)
     * @param passiveMode   Enable passive mode for FTP connections
     * @param userIsRoot    User dir is root for (s)ftp connections
     * @param cleanHdfsDir  Clean up HDFS temporary files (or  not)
     * @param codec         The compression codec to use, either gzip or bzip2
     * @param fileType      The output file type, either csv or json
     * @return A configured MR job object.
     * @throws Exception
     */
    public Job configure(boolean isSecured, String metaStoreUris, String principal,
                         String inputDatabase, String inputTable, String inputFilter, int numReducer,
                         String[] anonFields, String exportSalt, String keyFile, String ftpUser,
                         String ftpPass, String ftpEndpoint, String filePrefix, String delimiter,
                         boolean printHeader, boolean passiveMode, boolean userIsRoot,
                         boolean cleanHdfsDir, FileCompressionCodec codec, FileOutputType fileType) throws Exception {

        this.isSecured = isSecured;
        this.metaStoreUris = metaStoreUris;
        this.principal = principal;
        this.inputDatabase = inputDatabase;
        this.inputTable = inputTable;
        this.inputFilter = inputFilter;
        this.numReducer = numReducer;
        this.anonFields = anonFields.clone();
        this.exportSalt = exportSalt;
        this.keyFile = keyFile;
        this.ftpUser = ftpUser;
        this.ftpPass = ftpPass;
        this.ftpEndpoint = ftpEndpoint;
        this.filePrefix = filePrefix;
        this.delimiter = delimiter;
        this.printHeader = printHeader;
        this.passiveMode = passiveMode;
        this.userIsRoot = userIsRoot;
        this.cleanHdfsDir = cleanHdfsDir;
        this.codec = codec;
        this.fileType = fileType;

        return configure();
    }

    private Job configure() throws Exception {

        Configuration conf = getConfiguration();
        conf = configureHiveMetaStore(conf);
        conf = configureKerberos(conf);
        conf = configureAnonFields(conf);

        Job job = Job.getInstance(conf, "FtpExport: " + inputDatabase + "." + inputTable);

        job.setJarByClass(FtpExportJob.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(numReducer);

        if (inputFilter == null || inputFilter.trim().equals("")) {
            HCatInputFormat.setInput(job, inputDatabase, inputTable);

        } else {
            HCatInputFormat.setInput(job, inputDatabase, inputTable,
                    inputFilter);
        }

        if (filePrefix == null) {
            filePrefix = inputDatabase + "-" + inputTable;
        }

        FtpUploadOutputFormat.setOutput(job, inputTable, printHeader, delimiter,
                fileType, codec, ftpEndpoint, ftpUser, ftpPass, keyFile,
                filePrefix, passiveMode, userIsRoot, cleanHdfsDir);

        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(FtpUploadOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);

        if (fileType.equals(FileOutputType.csv)) {

            job.setOutputValueClass(TextPairArrayWritable.class);
            job.setMapperClass(FtpExportCSVMapper.class);
        } else if (fileType.equals(FileOutputType.json)) {

            HCatSchema hcatInputSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
            HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter();
            Schema schema = schemaConverter.convertSchema(hcatInputSchema, inputTable);
            AvroJob.setMapOutputValueSchema(job, schema);


            job.setOutputValueClass(AvroValue.class);
            job.setMapperClass(FtpExportJsonMapper.class);
        } else {
            throw new IllegalArgumentException("file output type must be either 'csv' or 'json'");
        }
        return job;
    }

    /**
     * The entry point when called from the command line.
     *
     * @param args A string array containing the cmdl args.
     * @throws Exception is thrown if an error occurs.
     */
    public static void main(String[] args) throws Exception {

        try {
            int exitCode = ToolRunner.run(new FtpExportJob(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }
    }
}
