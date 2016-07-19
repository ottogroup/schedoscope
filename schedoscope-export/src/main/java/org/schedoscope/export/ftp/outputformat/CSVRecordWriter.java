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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.schedoscope.export.writables.TextPairArrayWritable;

public class CSVRecordWriter<K, V> extends RecordWriter<K, V> {

	private DataOutputStream out;

	private CSVPrinter csvPrinter;

	private CSVFormat csvFormat;

	public CSVRecordWriter(DataOutputStream out, String[] header, char delimiter) {
		this.out = out;
		csvFormat = CSVFormat.DEFAULT
				.withTrim(true)
				.withQuoteMode(QuoteMode.ALL)
				.withHeader(header)
				.withDelimiter(delimiter);
	}

	@Override
	public void write(K key, V value) throws IOException {

		StringBuilder buffer = new StringBuilder();

		csvPrinter = csvFormat.print(buffer);
		csvPrinter.printRecord(((TextPairArrayWritable) value).getSecondAsList());
		out.write(buffer.toString().getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
		out.close();
	}
}