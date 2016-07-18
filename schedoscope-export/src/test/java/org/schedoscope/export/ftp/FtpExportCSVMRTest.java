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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.ftp.outputformat.CSVOutputFormat;
import org.schedoscope.export.ftp.upload.FileCompressionCodec;
import org.schedoscope.export.testsupport.EmbeddedFtpSftpServer;
import org.schedoscope.export.writables.TextPairArrayWritable;

public class FtpExportCSVMRTest extends HiveUnitBaseTest {

	private static final String HDFS_OUTPUT_DIR = "/tmp/output";

	private static EmbeddedFtpSftpServer server;

	private String filePrefix;

	@Override
	@Before
	public void setUp() throws Exception {

		super.setUp();
		filePrefix = RandomStringUtils.randomNumeric(20);
	}

	@BeforeClass()
	public static void setUpServer() throws Exception {

		server = new EmbeddedFtpSftpServer();
		server.startEmbeddedFtpServer();
		server.startEmbeddedSftpServer();
	}

	@AfterClass
	public static void tearDownServer() throws InterruptedException {

		server.stopEmbeddedFtpServer();
		server.stopEmbeddedSftpServer();
	}

	@Test
	public void testFtpCSVExport() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");



		Job job = Job.getInstance(conf);

		Path outfile = new Path(HDFS_OUTPUT_DIR);

		CSVOutputFormat.setOutputPath(job, outfile);
		CSVOutputFormat.setOutput(job, true, FileCompressionCodec.none, "ftp://localhost:2221/",
				EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING, EmbeddedFtpSftpServer.FTP_PASS_FOR_TESTING, null,
				filePrefix, true, true, true);

		job.setMapperClass(FtpExportCSVMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		assertTrue(job.waitForCompletion(true));
		assertEquals(2, getFileCount());
	}

	@Test
	public void testSftpCSVExportUserPassAuth() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

		Job job = Job.getInstance(conf);

		Path outfile = new Path(HDFS_OUTPUT_DIR);

		CSVOutputFormat.setOutputPath(job, outfile);
		CSVOutputFormat.setOutput(job, true, FileCompressionCodec.gzip, "sftp://localhost:12222/",
				EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING, EmbeddedFtpSftpServer.FTP_PASS_FOR_TESTING, null,
				filePrefix, true, true, true);

		job.setMapperClass(FtpExportCSVMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		assertTrue(job.waitForCompletion(true));
		assertEquals(2, getFileCount());
	}

	@Test
	public void testSftpCSVExportPubKeyAuthNoEnc() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

		Job job = Job.getInstance(conf);

		Path outfile = new Path(HDFS_OUTPUT_DIR);

		CSVOutputFormat.setOutputPath(job, outfile);
		CSVOutputFormat.setOutput(job, true, FileCompressionCodec.bzip2, "sftp://localhost:12222/",
				EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING, null, "src/test/resources/keys/id_rsa_not_encrypted",
				filePrefix, true, true, true);

		job.setMapperClass(FtpExportCSVMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		assertTrue(job.waitForCompletion(true));
		assertEquals(2, getFileCount());
	}

	@Test
	public void testSftpCSVExportPubKeyAuthEnc() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

		Job job = Job.getInstance(conf);

		Path outfile = new Path(HDFS_OUTPUT_DIR);

		CSVOutputFormat.setOutputPath(job, outfile);
		CSVOutputFormat.setOutput(job, true, FileCompressionCodec.gzip, "sftp://localhost:12222/",
				EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING, "12345",
				"src/test/resources/keys/id_rsa_encrypted",
				filePrefix, true, true, true);

		job.setMapperClass(FtpExportCSVMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(CSVOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TextPairArrayWritable.class);

		assertTrue(job.waitForCompletion(true));
		assertEquals(2, getFileCount());
	}

	private int getFileCount() throws IOException {

		FTPClient ftp = new FTPClient();
		ftp.connect("localhost", 2221);
		ftp.login(EmbeddedFtpSftpServer.FTP_USER_FOR_TESTING, EmbeddedFtpSftpServer.FTP_PASS_FOR_TESTING);
		FTPFile[] files = ftp.listFiles();

		int fileCounter = 0;
		for (FTPFile f : files) {
			if (f.getName().contains(filePrefix)) {
				fileCounter += 1;
			}
		}
		return fileCounter;
	}
}
