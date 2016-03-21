package org.schedoscope.export.kafka.outputformat;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import kafka.javaapi.producer.Producer;
import kafka.message.SnappyCompressionCodec;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * The Kafka output format is responsible to
 * write data into Kafka, it initializes the
 * KafkaRecordWriter.
 *
 * @param <K> The key class.
 * @param <V> The value class, must be a GenericRecord.
 */
public class KafkaOutputFormat<K, V extends GenericRecord> extends OutputFormat<K,V> {


    public static final String KAFKA_EXPORT_METADATA_BROKER_LIST = "metadata.broker.list";

    public static final String KAFKA_EXPORT_SERIALIZER_CLASS = "serializer.class";

    public static final String KAFKA_EXPORT_KEY_SERIALIZER_CLASS = "key.serializer.class";

    public static final String KAFKA_EXPORT_COMPRESSION_CODEC = "compression.codec";

    public static final String KAFKA_EXPORT_REQUEST_REQUIRED_ACKS = "request.required.acks";

    public static final String KAFKA_EXPORT_PRODUCER_TYPE = "producer.type";

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {

        return (new NullOutputFormat<NullWritable, NullWritable>()).getOutputCommitter(context);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {

        // Configuration conf = context.getConfiguration();

        Properties props = new Properties();
        props.setProperty(KAFKA_EXPORT_METADATA_BROKER_LIST, "localhost:9092");
        props.setProperty(KAFKA_EXPORT_SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        props.setProperty(KAFKA_EXPORT_KEY_SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        props.setProperty(KAFKA_EXPORT_COMPRESSION_CODEC, SnappyCompressionCodec.name());
        props.setProperty(KAFKA_EXPORT_PRODUCER_TYPE, "sync");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        return new KafkaStringRecordWriter(producer);
    }

    /**
     * The Kafka Record Writer is used to write data into
     * Kafka. It takes a GenericRecord but writes the JSON
     * representation into Kafka.
     */
    public class KafkaStringRecordWriter extends RecordWriter<K, V> {

        private Producer<String, String> producer;

        public KafkaStringRecordWriter(Producer<String, String> producer) {

            this.producer = producer;
        }

        @Override
        public void write(K key, V value) {
            KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic", value.toString());
            producer.send(message);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {

            producer.close();
        }
    }
}
