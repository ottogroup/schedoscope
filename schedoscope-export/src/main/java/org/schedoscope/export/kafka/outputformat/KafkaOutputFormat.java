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

package org.schedoscope.export.kafka.outputformat;

import java.io.IOException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;

/**
 * The Kafka output format is responsible to
 * write data into Kafka, it initializes the
 * KafkaRecordWriter.
 *
 * @param <K> The key class.
 * @param <V> The value class, must be a GenericRecord.
 */
public class KafkaOutputFormat<K extends Text, V extends AvroValue<GenericRecord>> extends OutputFormat<K,V> {

    // identifiers
    public static final String KAFKA_EXPORT_METADATA_BROKER_LIST = "metadata.broker.list";

    public static final String KAFKA_EXPORT_SERIALIZER_CLASS = "serializer.class";

    public static final String KAFKA_EXPORT_KEY_SERIALIZER_CLASS = "key.serializer.class";

    public static final String KAFKA_EXPORT_COMPRESSION_CODEC = "compression.codec";

    public static final String KAFKA_EXPORT_REQUEST_REQUIRED_ACKS = "request.required.acks";

    public static final String KAFKA_EXPORT_PRODUCER_TYPE = "producer.type";

    public static final String KAFKA_EXPORT_CLEANUP_POLICY = "cleanup.policy";

    public static final String KAFKA_EXPORT_KEY_NAME = "kafka.export.key.name";

    public static final String KAFKA_EXPORT_TABLE_NAME = "kafka.export.table.name";

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {

        return (new NullOutputFormat<NullWritable, NullWritable>()).getOutputCommitter(context);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {

        Configuration conf = context.getConfiguration();

        Properties producerProps = new Properties();
        producerProps.setProperty(KAFKA_EXPORT_METADATA_BROKER_LIST, conf.get(KAFKA_EXPORT_METADATA_BROKER_LIST));
        producerProps.setProperty(KAFKA_EXPORT_SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        producerProps.setProperty(KAFKA_EXPORT_KEY_SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        producerProps.setProperty(KAFKA_EXPORT_COMPRESSION_CODEC,
                conf.get(KAFKA_EXPORT_COMPRESSION_CODEC, CompressionCodec.gzip.toString()));
        producerProps.setProperty(KAFKA_EXPORT_PRODUCER_TYPE,
                conf.get(KAFKA_EXPORT_PRODUCER_TYPE, ProducerType.sync.toString()));
        producerProps.setProperty(KAFKA_EXPORT_REQUEST_REQUIRED_ACKS,
                conf.get(KAFKA_EXPORT_REQUEST_REQUIRED_ACKS, "1"));

        ProducerConfig config = new ProducerConfig(producerProps);
        Producer<String, String> producer = new Producer<String, String>(config);

        return new KafkaStringRecordWriter(producer, conf.get(KAFKA_EXPORT_TABLE_NAME));
    }

    /**
     * Initializes the KafkaOutputFormat.
     *
     * @param conf The Hadoop configuration object.
     * @param brokerList The list of Kafka brokers to bootstrap from.
     * @param zookeeperHosts The list of Zookeeper srvers to connect to.
     * @param producerType The Kafka producer type (sync / async).
     * @param cleanupPolicy The Kafka topic cleanup policy (delete / compact)
     * @param keyName The name of the key field.
     * @param tableName The name of the Hive table.
     * @param numPartitions The number of partitions for the given topic.
     * @param replicationFactor The replication factor for the given topic.
     * @param codec The compression codec to use (none / snappy / gzip).
     */
    public static void setOutput(Configuration conf, String brokerList, String zookeeperHosts,
            ProducerType producerType, CleanupPolicy cleanupPolicy, String keyName, String tableName,
            int numPartitions, int replicationFactor, CompressionCodec codec) {

        conf.set(KAFKA_EXPORT_METADATA_BROKER_LIST, brokerList);
        conf.set(KAFKA_EXPORT_PRODUCER_TYPE, producerType.toString());
        conf.set(KAFKA_EXPORT_CLEANUP_POLICY, cleanupPolicy.toString());
        conf.set(KAFKA_EXPORT_KEY_NAME, keyName);
        conf.set(KAFKA_EXPORT_TABLE_NAME, tableName);
        conf.set(KAFKA_EXPORT_COMPRESSION_CODEC, codec.toString());

        createOrUpdateTopic(zookeeperHosts, tableName, cleanupPolicy, numPartitions, replicationFactor);
    }

    private static void createOrUpdateTopic(String zookeeperHosts, String tableName,
            CleanupPolicy cleanupPolicy, int numPartitions, int replicationFactor) {

        Properties topicProps = new Properties();
        topicProps.setProperty(KAFKA_EXPORT_CLEANUP_POLICY, cleanupPolicy.toString());

        ZkClient zkClient = new ZkClient(zookeeperHosts, 30000, 30000, ZKStringSerializer$.MODULE$);
        if (AdminUtils.topicExists(zkClient, tableName)) {
            AdminUtils.changeTopicConfig(zkClient, tableName, topicProps);
        } else {
            AdminUtils.createTopic(zkClient, tableName, numPartitions, replicationFactor, topicProps);
        }
        zkClient.close();
    }

    /**
     * The Kafka Record Writer is used to write data into
     * Kafka. It takes a GenericRecord but writes the JSON
     * representation into Kafka.
     */
    public class KafkaStringRecordWriter extends RecordWriter<K, V> {

        private Producer<String, String> producer;

        private String topic;

        /**
         * Inializes a new Kafka Record Writer using
         * a Kafka producer under the hood.
         *
         * @param producer The configured Kafka producer.
         * @param topic The Kafka topic to send the data to.
         */
        public KafkaStringRecordWriter(Producer<String, String> producer, String topic) {

            this.producer = producer;
            this.topic = topic;
        }

        @Override
        public void write(K key, V value) {

            KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, value.datum().toString());
            producer.send(message);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {

            producer.close();
        }
    }
}
