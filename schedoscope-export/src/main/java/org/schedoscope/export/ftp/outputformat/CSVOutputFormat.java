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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.writables.TextPairArrayWritable;

import com.google.common.collect.Iterables;

public class CSVOutputFormat<K, V extends TextPairArrayWritable> extends FileOutputFormat<K, V> {

	private static final String FTP_EXPORT_DATE_TIME = "ftp.export.date.time";

	private static final String FTP_EXPORT_HEADER_COLUMNS = "ftp.export.header.columns";

	private CSVFileOutputCommitter committer = null;

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {

		Configuration conf = context.getConfiguration();

		Path file = getDefaultWorkFile(context, "");
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		String[] header = conf.getStrings(FTP_EXPORT_HEADER_COLUMNS);
		return new CSVRecordWriter<K, V>(fileOut, header);
	}

	public static void setOutput(Configuration conf, String timestamp, boolean printHeader) throws IOException {

		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2);
		conf.set(FTP_EXPORT_DATE_TIME, timestamp);

		if (printHeader) {
			conf.setStrings(FTP_EXPORT_HEADER_COLUMNS, setCSVHeader(conf));
		}
	}

	private static String[] setCSVHeader(Configuration conf) throws IOException {

		HCatSchema schema = HCatInputFormat.getTableSchema(conf);
		return Iterables.toArray(schema.getFieldNames(), String.class);
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
