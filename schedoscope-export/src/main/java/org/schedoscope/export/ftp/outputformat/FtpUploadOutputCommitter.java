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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.schedoscope.export.ftp.upload.Uploader;

import com.google.common.io.Files;

public class FtpUploadOutputCommitter extends FileOutputCommitter {

	private static final String TMP_FILE_PREFIX = "private_key_";

	private static final String TMP_FILE_SUFFIX = ".rsa";

	private Uploader uploader;

	private Path outputPath;

	private String endpoint;

	private String filePrefix;

	private String user;

	private String pass;

	private String keyContent;

	private int numReducer;

	private boolean passiveMode;

	private boolean userIsRoot;

	private boolean cleanHdfsDir;

	public FtpUploadOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {

		super(outputPath, context);

		Configuration conf = context.getConfiguration();

		this.outputPath = outputPath;
		this.endpoint = conf.get(FtpUploadOutputFormat.FTP_EXPORT_ENDPOINT);
		this.filePrefix = conf.get(FtpUploadOutputFormat.FTP_EXPORT_FILE_PREFIX);

		this.user = conf.get(FtpUploadOutputFormat.FTP_EXPORT_USER);
		this.pass = conf.get(FtpUploadOutputFormat.FTP_EXPORT_PASS);
		this.keyContent = conf.get(FtpUploadOutputFormat.FTP_EXPORT_KEY_FILE_CONTENT);
		this.passiveMode = conf.getBoolean(FtpUploadOutputFormat.FTP_EXPORT_PASSIVE_MODE, true);
		this.userIsRoot = conf.getBoolean(FtpUploadOutputFormat.FTP_EXPORT_USER_IS_ROOT, true);
		this.cleanHdfsDir = conf.getBoolean(FtpUploadOutputFormat.FTP_EXPORT_CLEAN_HDFS_DIR, true);
		this.numReducer = context.getNumReduceTasks();

		try {

			String protocol = new URI(endpoint).getScheme();

			if (!protocol.equals("ftp") && !protocol.equals("sftp")) {
				throw new IllegalArgumentException("protocol not supported, must be either 'ftp' or 'sftp'");
			}

		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {

		super.commitTask(context);

		String fileName = FtpUploadOutputFormat.getOutputName(context);
		String remote = endpoint + "/" + filePrefix + context.getTaskAttemptID().getTaskID().getId() + "-" + numReducer + FtpUploadOutputFormat.getOutputNameExtension();

		Configuration conf = context.getConfiguration();

		if (keyContent != null && !keyContent.isEmpty()) {

			File keyFile = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_SUFFIX);
			keyFile.deleteOnExit();
			Files.write(keyContent.getBytes(StandardCharsets.US_ASCII), keyFile);

			uploader = new Uploader(user, new File(keyFile.getCanonicalPath()), pass, conf, passiveMode, userIsRoot);
		} else {
			uploader = new Uploader(user, pass, conf, passiveMode, userIsRoot);

		}
		uploader.uploadFile(new Path(outputPath, fileName).toString(), remote);
		uploader.closeFilesystem();
	}

	@Override
	public void commitJob(JobContext context) throws IOException {

		if (cleanHdfsDir) {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			fs.delete(outputPath, true);
		}
	}
}
