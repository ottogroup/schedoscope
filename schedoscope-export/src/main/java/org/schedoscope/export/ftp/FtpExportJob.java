package org.schedoscope.export.ftp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.schedoscope.export.BaseExportJob;

public class FtpExportJob extends BaseExportJob {

	private static final Log LOG = LogFactory.getLog(FtpExportJob.class);

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

		job.setInputFormatClass(HCatInputFormat.class);

		return job;
	}
}
