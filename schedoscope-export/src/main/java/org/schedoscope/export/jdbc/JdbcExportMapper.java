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

package org.schedoscope.export.jdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputWritable;
import org.schedoscope.export.jdbc.outputschema.Schema;
import org.schedoscope.export.jdbc.outputschema.SchemaFactory;
import org.schedoscope.export.utils.HCatRecordJsonSerializer;
import org.schedoscope.export.utils.HCatUtils;

import com.google.common.collect.ImmutableSet;

/**
 * A mapper that reads data from Hive via HCatalog and emits a JDBC writable..
 */
public class JdbcExportMapper
		extends
		Mapper<WritableComparable<?>, HCatRecord, LongWritable, JdbcOutputWritable> {

	private static final Log LOG = LogFactory.getLog(JdbcExportMapper.class);

	private String[] columnTypes;

	private Map<String, String> typeMapping;

	private HCatSchema inputSchema;

	private String inputFilter;

	private Configuration conf;

	private HCatRecordJsonSerializer serializer;

	private Set<String> anonFields;

	private String salt;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		inputSchema = HCatInputFormat
				.getTableSchema(context.getConfiguration());

		serializer = new HCatRecordJsonSerializer(conf, inputSchema);

		Schema outputSchema = SchemaFactory.getSchema(context
				.getConfiguration());

		inputFilter = outputSchema.getFilter();

		columnTypes = outputSchema.getColumnTypes();

		typeMapping = outputSchema.getPreparedStatementTypeMapping();

		anonFields = ImmutableSet.copyOf(conf.getStrings(
				BaseExportJob.EXPORT_ANON_FIELDS, new String[0]));

		salt = conf.get(BaseExportJob.EXPORT_ANON_SALT, "");

		LOG.info("Used Filter: " + inputFilter);
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value,
			Context context) throws IOException, InterruptedException {

		List<Pair<String, String>> record = new ArrayList<Pair<String, String>>();

		for (String f : inputSchema.getFieldNames()) {

			String fieldValue = "NULL";
			String fieldType = typeMapping.get(columnTypes[inputSchema
					.getPosition(f)]);

			Object obj = value.get(f, inputSchema);
			if (obj != null) {

				if (inputSchema.get(f).isComplex()) {
					fieldValue = serializer.getFieldAsJson(value, f);
				} else {
					fieldValue = obj.toString();
					fieldValue = HCatUtils.getHashValueIfInList(f, fieldValue,
							anonFields, salt);
				}
			}
			record.add(Pair.of(fieldType, fieldValue));
		}

		String filterType = typeMapping
				.get(columnTypes[columnTypes.length - 1]);
		if (inputFilter == null) {
			record.add(Pair.of(filterType, "NULL"));
		} else {
			record.add(Pair.of(filterType, inputFilter));
		}

		LongWritable localKey = new LongWritable(context.getCounter(
				TaskCounter.MAP_INPUT_RECORDS).getValue());
		context.write(localKey, new JdbcOutputWritable(record));
	}
}
