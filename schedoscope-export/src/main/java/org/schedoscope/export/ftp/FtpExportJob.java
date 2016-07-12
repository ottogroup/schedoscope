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
import org.apache.hadoop.fs.FileSystem;
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
import org.schedoscope.export.ftp.upload.Uploader;
import org.schedoscope.export.writables.TextPairArrayWritable;



public class FtpExportJob extends BaseExportJob {

	private static final Log LOG = LogFactory.getLog(FtpExportJob.class);

	private static final String LOCAL_PATH_PREFIX = "file://";

	private static final String PRIVATE_KEY_FILE_NAME = "id_rsa";

	private static final String TMP_OUTPUT_PATH = "/tmp/export";

	@Option(name = "-k", usage = "ssh private key file")
	private String keyFile = "~/.ssh/id_rsa";

	@Option(name = "-u", usage = "the (s)ftp user")
	private String ftpUser;

	@Option(name = "-w", usage = "the ftp password / sftp passphrase")
	private String ftpPass;

	@Option(name = "-j", usage = "the (s)ftp endpoint, e.g. ftp://ftp.example.com:21/path/to/")
	private String ftpEndpoint;

	@Option(name = "-x", usage = "passive mode, only for ftp connections")
	private boolean passiveMode = true;

	@Option(name = "-z", usage = "user dir is root, home dir on remote end is (s)ftp root dir")
	private boolean userIsRoot = true;

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

	private Job configure() throws Exception {

		Uploader.checkPrivateKey(keyFile);

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

		FileSystem fs = FileSystem.get(job.getConfiguration());
		String tmpDir = job.getConfiguration().get("hadoop.tmp.dir");
		String hdfsKeyFile = null;
		if (tmpDir != null) {
			LOG.info("copy " + LOCAL_PATH_PREFIX + keyFile + " to " + tmpDir);
			hdfsKeyFile = tmpDir + "/" + PRIVATE_KEY_FILE_NAME;
			fs.copyFromLocalFile(false,  true, new Path(LOCAL_PATH_PREFIX, keyFile), new Path(hdfsKeyFile));
		} else {
			throw new IllegalArgumentException("hadoop tmp dir not defined");
		}

		String filePrefix = inputDatabase + "_" + inputTable;
		CSVOutputFormat.setOutputPath(job, new Path(TMP_OUTPUT_PATH));
		CSVOutputFormat.setOutput(job, true, true, ftpEndpoint, ftpUser, ftpPass, hdfsKeyFile, filePrefix);

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
