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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.RandomStringUtils;
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
import org.schedoscope.export.ftp.upload.FileCompressionCodec;

import com.google.common.collect.Iterables;

/**
 * The FtpUpload output format is responsible to set up the record writers and
 * (s)ftp connection settings.
 */
public class FtpUploadOutputFormat<K, V> extends FileOutputFormat<K, V> {

	private static final Log LOG = LogFactory.getLog(FtpUploadOutputFormat.class);

	public static final String FTP_EXPORT_TMP_OUTPUT_PATH = "export_";

	public static final String FTP_EXPORT_TABLE_NAME = "ftp.export.table.name";

	public static final String FTP_EXPORT_FILE_PREFIX = "ftp.export.file.prefix";

	public static final String FTP_EXPORT_USER = "ftp.export.user";

	public static final String FTP_EXPORT_PASS = "ftp.export.pass";

	public static final String FTP_EXPORT_KEY_FILE_CONTENT = "ftp.export.key.file.content";

	public static final String FTP_EXPORT_ENDPOINT = "ftp.export.endpoint";

	public static final String FTP_EXPORT_PASSIVE_MODE = "ftp.export.passive.mode";

	public static final String FTP_EXPORT_USER_IS_ROOT = "ftp.export.user.is.root";

	public static final String FTP_EXPORT_CLEAN_HDFS_DIR = "ftp.export.clean.hdfs.dir";

	public static final String FTP_EXPORT_CVS_DELIMITER = "ftp.export.csv.delimmiter";

	private static final String FTP_EXPORT_HEADER_COLUMNS = "ftp.export.header.columns";

	private static final String FTP_EXPORT_FILE_TYPE = "ftp.export.file.type";

	private FtpUploadOutputCommitter committer = null;

	private static String extension = "";

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {

		Configuration conf = context.getConfiguration();

		boolean isCompressed = getCompressOutput(context);
		CompressionCodec codec = null;

		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);

			// only support gzip and bzip2 compression
			if (codecClass.equals(BZip2Codec.class) || codecClass.equals(GzipCodec.class)) {
				codec = ReflectionUtils.newInstance(codecClass, conf);
				extension = codec.getDefaultExtension();
			} else {
				LOG.warn("neither gzip nor bzip2 compression codec found - disabling compression");
				isCompressed = false;
				extension = "";
			}
		}

		char delimiter = conf.get(FTP_EXPORT_CVS_DELIMITER, "\t").charAt(0);
		String[] header = conf.getStrings(FTP_EXPORT_HEADER_COLUMNS);

		Path file = getDefaultWorkFile(context, extension);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);

		RecordWriter<K, V> writer;

		if (conf.get(FTP_EXPORT_FILE_TYPE).equals(FileOutputType.csv.toString())) {

			if (!isCompressed) {
				writer = new CSVRecordWriter<K, V>(fileOut, header, delimiter);
			} else {
				writer = new CSVRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)), header, delimiter);
			}

		} else if (conf.get(FTP_EXPORT_FILE_TYPE).equals(FileOutputType.json.toString())) {

			if (!isCompressed) {
				writer = new JsonRecordWriter<K, V>(fileOut);
			} else {
				writer = new JsonRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)));
			}

		} else {
			throw new IllegalArgumentException("unknown file output type");
		}

		return writer;
	}

	/**
	 * A method to configure the output format.
	 * @param job The job object.
	 * @param tableName The Hive input table name
	 * @param printHeader A flag indicating to print a csv header or not.
	 * @param delimiter The delimiter to use for separating the records (CSV)
	 * @param fileType The file type (csv / json)
	 * @param codec The compresson codec (none / gzip / bzip2)
	 * @param ftpEndpoint The (s)ftp endpoint.
	 * @param ftpUser The (s)ftp user
	 * @param ftpPass The (s)ftp password or sftp passphrase
	 * @param keyFile The private ssh key file
	 * @param filePrefix An optional file prefix
	 * @param passiveMode Passive mode or not (only ftp)
	 * @param userIsRoot User dir is root or not
	 * @param cleanHdfsDir Clean up HDFS temporary files.
	 * @throws Exception Is thrown if an error occurs.
	 */
	public static void setOutput(Job job, String tableName, boolean printHeader, String delimiter, FileOutputType fileType,
			FileCompressionCodec codec, String ftpEndpoint, String ftpUser, String ftpPass, String keyFile, String filePrefix,
			boolean passiveMode, boolean userIsRoot, boolean cleanHdfsDir) throws Exception {

		Configuration conf = job.getConfiguration();
		String tmpDir = conf.get("hadoop.tmp.dir");
		String localTmpDir = RandomStringUtils.randomNumeric(10);
		setOutputPath(job, new Path(tmpDir, FTP_EXPORT_TMP_OUTPUT_PATH + localTmpDir));

		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2);

		conf.set(FTP_EXPORT_TABLE_NAME, tableName);

		conf.set(FTP_EXPORT_ENDPOINT, ftpEndpoint);
		conf.set(FTP_EXPORT_USER, ftpUser);

		if (ftpPass != null) {
			conf.set(FTP_EXPORT_PASS, ftpPass);
		}

		if (delimiter != null) {
			if (delimiter.length() != 1) {
				throw new IllegalArgumentException("delimiter must be exactly 1 char");
			}
			conf.set(FTP_EXPORT_CVS_DELIMITER, delimiter);
		}

		if (keyFile != null && Files.exists(Paths.get(keyFile))) {

			// Uploader.checkPrivateKey(keyFile);
			String privateKey = new String(Files.readAllBytes(Paths.get(keyFile)), StandardCharsets.US_ASCII);
			conf.set(FTP_EXPORT_KEY_FILE_CONTENT, privateKey);
		}

		conf.setBoolean(FTP_EXPORT_PASSIVE_MODE, passiveMode);
		conf.setBoolean(FTP_EXPORT_USER_IS_ROOT, userIsRoot);
		conf.setBoolean(FTP_EXPORT_CLEAN_HDFS_DIR, cleanHdfsDir);

		DateTimeFormatter fmt = ISODateTimeFormat.basicDateTimeNoMillis();
		String timestamp = fmt.print(DateTime.now(DateTimeZone.UTC));
		conf.set(FTP_EXPORT_FILE_PREFIX, filePrefix + "-" + timestamp + "-");

		if (printHeader) {
			conf.setStrings(FTP_EXPORT_HEADER_COLUMNS, setCSVHeader(conf));
		}

		conf.set(FTP_EXPORT_FILE_TYPE, fileType.toString());

		if (codec.equals(FileCompressionCodec.gzip)) {
			setOutputCompressorClass(job, GzipCodec.class);
		} else if (codec.equals(FileCompressionCodec.bzip2)) {
			setOutputCompressorClass(job, BZip2Codec.class);
		} else if (codec.equals(FileCompressionCodec.none)) {
			extension = "";
		}
	}

	private static String[] setCSVHeader(Configuration conf) throws IOException {

		HCatSchema schema = HCatInputFormat.getTableSchema(conf);
		return Iterables.toArray(schema.getFieldNames(), String.class);
	}

	/**
	 * A method to provide the fully qualified file path of the current file.
	 * @param context The TaskAttemptContext.
	 * @return Returns the fully qualified file path of the current file.
	 */
	public static String getOutputName(TaskAttemptContext context) {
		return getUniqueFile(context, FileOutputFormat.getOutputName(context), extension);
	}

	/**
	 * A method to return the file extension, depends on the compression codec.
	 * @return The file extension.
	 */
	public static String getOutputNameExtension() {
		return extension;
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {

		if (committer == null) {
			Path output = getOutputPath(context);
			committer = new FtpUploadOutputCommitter(output, context);
		}
		return committer;
	}
}
