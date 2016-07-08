package org.schedoscope.export.ftp;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.ftp.outputformat.CSVOutputFormat;
import org.schedoscope.export.writables.TextPairArrayWritable;

public class FtpExportCSVMRTest extends HiveUnitBaseTest {

	private static final String OUTPUT_DIR = "/tmp/putput";

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
	}

	@Test
	public void testFtpCSVExport() throws Exception {
		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);



		Job job = Job.getInstance(conf);

		Path outfile = new Path(OUTPUT_DIR);

		CSVOutputFormat.setOutputPath(job, outfile);

		DateTimeFormatter fmt = ISODateTimeFormat.basicDateTimeNoMillis();
		String timestamp = fmt.print(DateTime.now(DateTimeZone.UTC));

		CSVOutputFormat.setOutput(job, timestamp, true, true);

		job.setMapperClass(FtpExportCSVMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		assertTrue(job.waitForCompletion(true));

		FileSystem fs = outfile.getFileSystem(conf);

		RemoteIterator<LocatedFileStatus> stat = fs.listFiles(outfile, true);

		while (stat.hasNext()) {
			System.out.println(stat.next());
		}
	}
}
