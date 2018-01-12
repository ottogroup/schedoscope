package org.schedoscope.export.bigquery;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

public class BigQueryExportMapper extends Mapper<WritableComparable<?>, HCatRecord, LongWritable, HCatRecord> {
    @Override
    protected void map(WritableComparable<?> key, HCatRecord value,
                       Context context) throws IOException, InterruptedException {

        LongWritable localKey = new LongWritable(context.getCounter(
                TaskCounter.MAP_INPUT_RECORDS).getValue());

        context.write(localKey, value);
    }
}
