package org.schedoscope.export.ftp.outputformat;

import org.apache.hadoop.io.ArrayWritable;
import org.schedoscope.export.jdbc.outputformat.TextPairWritable;

public class TextPairArrayWritable extends ArrayWritable {

	public TextPairArrayWritable() {
		super(TextPairWritable.class);
	}

	public TextPairArrayWritable(TextPairWritable[] data) {
		super(TextPairWritable.class, data);
	}
}
