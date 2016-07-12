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

package org.schedoscope.export.ftp.outputformat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.schedoscope.export.writables.TextPairArrayWritable;

import com.google.common.collect.Iterables;

public class CSVOutputFormat<K, V extends TextPairArrayWritable> extends FileOutputFormat<K, V> {

	private static final Log LOG = LogFactory.getLog(CSVOutputFormat.class);

	public static final String FTP_EXPORT_FILE_PREFIX = "ftp.export.file.prefix";

	public static final String FTP_EXPORT_USER = "ftp.export.user";

	public static final String FTP_EXPORT_PASS = "ftp.export.pass";

	public static final String FTP_EXPORT_KEY_FILE = "ftp.export.key.file";

	public static final String FTP_EXPORT_ENDPOINT = "ftp.export.endpoint";

	public static final String FTP_EXPORT_PASSIVE_MODE = "ftp.export.passive.mode";

	public static final String FTP_EXPORT_USER_IS_ROOT = "ftp.export.user.is.root";

	private static final String FTP_EXPORT_HEADER_COLUMNS = "ftp.export.header.columns";

	private CSVFileOutputCommitter committer = null;

	private static String extension = "";

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {

		Configuration conf = context.getConfiguration();

		boolean isCompressed = getCompressOutput(context);

		CompressionCodec codec = null;

		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}

		// only support gzip and bzip2 compression
		if (!codec.getCompressorType().equals(BZip2Codec.class) && !codec.getCompressorType().equals(GzipCodec.class)) {
			LOG.warn("neither gzip nor bzip2 compression codec found - disabling compression");
			isCompressed = false;
			extension = "";
		}

		String[] header = conf.getStrings(FTP_EXPORT_HEADER_COLUMNS);

		Path file = getDefaultWorkFile(context, extension);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);

		if (!isCompressed) {
			return new CSVRecordWriter<K, V>(fileOut, header);
		} else {
			return new CSVRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)), header);
		}
	}

	public static void setOutput(Job job, boolean printHeader, boolean compress, String ftpEndpoint, String ftpUser, String ftpPass,
			String keyFile, String filePrefix) throws IOException {

		Configuration conf = job.getConfiguration();
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2);

		conf.set(FTP_EXPORT_ENDPOINT, ftpEndpoint);
		conf.set(FTP_EXPORT_USER, ftpUser);
		conf.set(FTP_EXPORT_PASS, ftpPass);
		if (keyFile != null) {
			conf.set(FTP_EXPORT_KEY_FILE, keyFile);
		}

		DateTimeFormatter fmt = ISODateTimeFormat.basicDateTimeNoMillis();
		String timestamp = fmt.print(DateTime.now(DateTimeZone.UTC));
		conf.set(FTP_EXPORT_FILE_PREFIX, filePrefix + "-" + timestamp + "-");

		if (printHeader) {
			conf.setStrings(FTP_EXPORT_HEADER_COLUMNS, setCSVHeader(conf));
		}

		setCompressOutput(job, compress);
		setOutputCompressorClass(job, GzipCodec.class);
	}

	private static String[] setCSVHeader(Configuration conf) throws IOException {

		HCatSchema schema = HCatInputFormat.getTableSchema(conf);
		return Iterables.toArray(schema.getFieldNames(), String.class);
	}

	public static String getOutputName(TaskAttemptContext context) {
		return getUniqueFile(context, FileOutputFormat.getOutputName(context), extension);
	}

	public static String getOutputNameExtension() {
		return extension;
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {

		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new CSVFileOutputCommitter(output, context);
		}
		return committer;
	}
}
