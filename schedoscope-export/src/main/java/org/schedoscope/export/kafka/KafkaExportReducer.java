package org.schedoscope.export.kafka;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer to write data into Kafka.
 */
public class KafkaExportReducer extends Reducer<Text, AvroValue<GenericRecord>, String, GenericRecord> {

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context)
            throws IOException, InterruptedException {

        for (AvroValue<GenericRecord> r : values) {
            context.write(key.toString(), r.datum());
        }
    }
}
