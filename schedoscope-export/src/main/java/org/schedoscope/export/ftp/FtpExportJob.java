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

package org.schedoscope.export.ftp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.ftp.outputformat.CSVOutputFormat;
import org.schedoscope.export.ftp.upload.FileCompressionCodec;
import org.schedoscope.export.writables.TextPairArrayWritable;

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

	@Option(name = "-z", usage = "file prefix for files send to (s)ftp server, defaults to 'database'-'table'")
	private String filePrefix;

	@Option(name = "-l", usage = "delimiter for csv export, defaults to '\t'")
	private String delimiter = "\t";

	@Option(name = "-h", usage = "print header in exported CSV files")
	private boolean printHeader;

	@Option(name = "-x", usage = "passive mode, only for ftp connections, defaults to 'true'")
	private boolean passiveMode = true;

	@Option(name = "-z", usage = "user dir is root, home dir on remote end is (s)ftp root dir defaults to 'true'")
	private boolean userIsRoot = true;

	@Option(name = "-g", usage = "clean up hdfs dir after export, defaults to 'true'")
	private boolean cleanHdfsDir = true;

	@Option(name = "-c", usage = "compression codec, either 'none', 'gzip' or 'bzip2', defaults to 'gzip'")
	private FileCompressionCodec codec = FileCompressionCodec.gzip;

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

	public Job configure(boolean isSecured, String metaStoreUris, String principal,
			String inputDatabase, String inputTable, String inputFilter, int numReducer,
			String[] anonFields, String exportSalt, String keyFile, String ftpUser,
			String ftpPass, String ftpEndpoint, String filePrefix, String delimiter,
			boolean printHeader, boolean passiveMode, boolean userIsRoot,
			boolean cleanHdfsDir, FileCompressionCodec codec) throws Exception {

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

		return configure();
	}

	private Job configure() throws Exception {

		Configuration conf = getConfiguration();
		conf = configureHiveMetaStore(conf);
		conf = configureKerberos(conf);
		conf = configureAnonFields(conf);

		Job job = Job.getInstance(conf, "FtpExport: " + inputDatabase + "." + inputTable);
		job.setJarByClass(FtpExportJob.class);
		job.setMapperClass(FtpExportCSVMapper.class);
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

		String tmpDir = job.getConfiguration().get("hadoop.tmp.dir");

		CSVOutputFormat.setOutputPath(job, new Path(tmpDir, CSVOutputFormat.FTP_EXPORT_TMP_OUTPUT_PATH));
		CSVOutputFormat.setOutput(job, printHeader, delimiter, codec, ftpEndpoint, ftpUser, ftpPass, keyFile, filePrefix, passiveMode, userIsRoot, cleanHdfsDir);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		return job;
	}

	public static void main (String[] args) throws Exception {

		try {
			int exitCode = ToolRunner.run(new FtpExportJob(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
	}
}
