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

package org.schedoscope.export.kafka;

import java.io.IOException;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.kafka.avro.HCatToAvroRecordConverter;
import org.schedoscope.export.kafka.avro.HCatToAvroSchemaConverter;
import org.schedoscope.export.kafka.outputformat.KafkaOutputFormat;
import org.schedoscope.export.utils.HCatRecordJsonSerializer;
import org.schedoscope.export.utils.HCatUtils;

import com.google.common.collect.ImmutableSet;

/**
 * A mapper that reads data from Hive tables and emits a GenericRecord.
 */
public class KafkaExportMapper
		extends
		Mapper<WritableComparable<?>, HCatRecord, Text, AvroValue<GenericRecord>> {

	private String tableName;

	private HCatSchema hcatSchema;

	private String keyName;

	private HCatToAvroRecordConverter converter;

	private Schema avroSchema;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		super.setup(context);
		Configuration conf = context.getConfiguration();
		hcatSchema = HCatInputFormat.getTableSchema(conf);

		keyName = conf.get(KafkaOutputFormat.KAFKA_EXPORT_KEY_NAME);
		tableName = conf.get(KafkaOutputFormat.KAFKA_EXPORT_TABLE_NAME);

		HCatUtils.checkKeyType(hcatSchema, keyName);

		Set<String> anonFields = ImmutableSet.copyOf(conf.getStrings(
				BaseExportJob.EXPORT_ANON_FIELDS, new String[0]));
		String salt = conf.get(BaseExportJob.EXPORT_ANON_SALT, "");
		HCatRecordJsonSerializer serializer = new HCatRecordJsonSerializer(
				conf, hcatSchema);
		converter = new HCatToAvroRecordConverter(serializer, anonFields, salt);

		HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter(
				anonFields);
		avroSchema = schemaConverter.convertSchema(hcatSchema, tableName);
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value,
			Context context) throws IOException, InterruptedException {

		Text kafkaKey = new Text(value.getString(keyName, hcatSchema));
		GenericRecord record = converter.convert(value, avroSchema);
		AvroValue<GenericRecord> recordWrapper = new AvroValue<GenericRecord>(
				record);

		context.write(kafkaKey, recordWrapper);
	}
}
