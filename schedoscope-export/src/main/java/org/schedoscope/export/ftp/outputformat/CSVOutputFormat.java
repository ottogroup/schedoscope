package org.schedoscope.export.ftp.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class CSVOutputFormat<K, V extends Text> extends OutputFormat<K, V> {

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {

		return (new NullOutputFormat<NullWritable, NullWritable>())
				.getOutputCommitter(context);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {

		Configuration conf = context.getConfiguration();

		return new CSVRecordWriter();
	}

	public class CSVRecordWriter extends RecordWriter<K,V> {

		public CSVRecordWriter() {

		}

		@Override
		public void write(K key, V value) {

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {

		}
	}
}
