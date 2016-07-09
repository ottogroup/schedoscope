package org.schedoscope.export.ftp.outputformat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.schedoscope.export.ftp.upload.SFTPUploader;

public class CSVFileOutputCommitter extends FileOutputCommitter {

	private Path outputPath;

	private SFTPUploader uploader;

	public CSVFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {

		super(outputPath, context);
		this.outputPath = outputPath;

	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {

		super.commitTask(context);

		String fileName = CSVOutputFormat.getOutputName(context);

		try {
			URI remote = new URI("ftp://vagrant:vagrant@192.168.56.101/home/vagrant/" + context.getTaskAttemptID().getTaskID().getId());
			uploader = new SFTPUploader(remote, "vagrant", "vagrant", "", true, false, context.getConfiguration());

			uploader.uploadFile(new Path(outputPath, fileName + ".deflate").toString(), remote.toString());

		} catch (URISyntaxException e) {

		}

	}
}
