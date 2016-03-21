package org.schedoscope.export.kafka;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.kafka.avro.HCatToAvroRecordConverter;

/**
 * A mapper that reads data from Hive tables and emits a GenericRecord.
 */
public class KafkaExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, AvroValue<GenericRecord>> {

    String tableName;

    HCatSchema hcatSchema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);

        Configuration conf = context.getConfiguration();
        hcatSchema = HCatInputFormat.getTableSchema(conf);

        tableName = "MyTable";
    }

    @Override
    protected void map(WritableComparable<?> key, HCatRecord value, Context context)
            throws IOException, InterruptedException {

        GenericRecord record = HCatToAvroRecordConverter.convertRecord(value, hcatSchema, tableName);
        AvroValue<GenericRecord> recordWrapper = new AvroValue<GenericRecord>(record);

        Text localKey = new Text();
        context.write(localKey, recordWrapper);
    }
}
