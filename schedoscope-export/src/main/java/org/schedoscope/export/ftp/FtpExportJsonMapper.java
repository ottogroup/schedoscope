package org.schedoscope.export.ftp;

import java.io.IOException;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.ftp.outputformat.FtpUploadOutputFormat;
import org.schedoscope.export.kafka.avro.HCatToAvroRecordConverter;
import org.schedoscope.export.kafka.avro.HCatToAvroSchemaConverter;
import org.schedoscope.export.utils.HCatRecordJsonSerializer;

import com.google.common.collect.ImmutableSet;

public class FtpExportJsonMapper extends Mapper<WritableComparable<?>, HCatRecord, LongWritable, AvroValue<GenericRecord>> {

	private String tableName;

	private HCatSchema hcatSchema;

	private HCatToAvroRecordConverter converter;

	private Schema avroSchema;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		Configuration conf = context.getConfiguration();
		hcatSchema = HCatInputFormat.getTableSchema(conf);

		tableName = conf.get(FtpUploadOutputFormat.FTP_EXPORT_TABLE_NAME);

		Set<String> anonFields = ImmutableSet.copyOf(conf.getStrings(BaseExportJob.EXPORT_ANON_FIELDS, new String[0]));

		String salt = conf.get(BaseExportJob.EXPORT_ANON_SALT, "");

		HCatRecordJsonSerializer serializer = new HCatRecordJsonSerializer(conf, hcatSchema);

		converter = new HCatToAvroRecordConverter(serializer, anonFields, salt);

		HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter(anonFields);
		avroSchema = schemaConverter.convertSchema(hcatSchema, tableName);
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException {

		GenericRecord record = converter.convert(value, avroSchema);
		AvroValue<GenericRecord> recordWrapper = new AvroValue<GenericRecord>(record);

		LongWritable localKey = new LongWritable(context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
		context.write(localKey, recordWrapper);
	}
}
