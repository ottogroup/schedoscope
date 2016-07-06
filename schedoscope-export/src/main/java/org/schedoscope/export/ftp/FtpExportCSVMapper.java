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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.ftp.outputformat.TextPairArrayWritable;
import org.schedoscope.export.jdbc.outputformat.TextPairWritable;
import org.schedoscope.export.utils.HCatRecordJsonSerializer;
import org.schedoscope.export.utils.HCatUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class FtpExportCSVMapper extends Mapper<WritableComparable<?>, HCatRecord, LongWritable, TextPairArrayWritable> {

	private Configuration conf;

	private HCatSchema inputSchema;

	private HCatRecordJsonSerializer serializer;

	private Set<String> anonFields;

	private String salt;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();

		inputSchema = HCatInputFormat.getTableSchema(conf);

		serializer = new HCatRecordJsonSerializer(conf, inputSchema);

		anonFields = ImmutableSet.copyOf(conf.getStrings(BaseExportJob.EXPORT_ANON_FIELDS, new String[0]));

		salt = conf.get(BaseExportJob.EXPORT_ANON_SALT, "");
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context)
			throws IOException, InterruptedException {

		List<TextPairWritable> items = new ArrayList<TextPairWritable>();

		for (String f : inputSchema.getFieldNames()) {

			String fieldValue = "";

			Object obj = value.get(f, inputSchema);
			if (obj != null) {

				if (inputSchema.get(f).isComplex()) {
					fieldValue = serializer.getFieldAsJson(value, f);
				} else {
					fieldValue = obj.toString();
					fieldValue = HCatUtils.getHashValueIfInList(f, fieldValue, anonFields, salt);
				}
			}

			TextPairWritable item = new TextPairWritable(f, fieldValue);
			items.add(item);
		}

		TextPairArrayWritable record = new TextPairArrayWritable(Iterables.toArray(items, TextPairWritable.class));

		LongWritable localKey = new LongWritable(context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
		context.write(localKey, record);
	}
}
