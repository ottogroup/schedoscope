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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.ftp.outputformat.CSVOutputFormat;
import org.schedoscope.export.writables.TextPairArrayWritable;

public class FtpExportJob extends BaseExportJob {

	// private static final Log LOG = LogFactory.getLog(FtpExportJob.class);

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
		job.setMapperClass(FtpExportCSVMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(numReducer);

		if (inputFilter == null || inputFilter.trim().equals("")) {
			HCatInputFormat.setInput(job, inputDatabase, inputTable);

		} else {
			HCatInputFormat.setInput(job, inputDatabase, inputTable,
					inputFilter);
		}

		CSVOutputFormat.setOutputPath(job, new Path("/tmp/richter"));

		DateTimeFormatter fmt = ISODateTimeFormat.basicDateTimeNoMillis();
		String timestamp = fmt.print(DateTime.now(DateTimeZone.UTC));
		CSVOutputFormat.setOutput(job, timestamp, true, true);

		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		return job;
	}
}
