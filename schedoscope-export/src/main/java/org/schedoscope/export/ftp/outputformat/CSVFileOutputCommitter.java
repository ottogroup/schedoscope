package org.schedoscope.export.ftp.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class CSVFileOutputCommitter extends FileOutputCommitter {

	private Path outputPath;

	public CSVFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
		super(outputPath, context);
		this.outputPath = outputPath;
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {

		super.commitTask(context);
		Path taskAttemptPath = getTaskAttemptPath(context);
		FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());

		Path fileName = new Path(outputPath, taskAttemptPath.getName());
		FileStatus finalFileStatus = fs.getFileStatus(taskAttemptPath);
		finalFileStatus.getPath();

	}
}
