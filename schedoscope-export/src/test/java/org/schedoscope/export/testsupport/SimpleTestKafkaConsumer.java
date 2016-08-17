/**
 * Copyright 2016 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.testsupport;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

public class SimpleTestKafkaConsumer implements Iterable<byte[]> {

    private ConsumerConnector consumer;

    final ConsumerIterator<byte[], byte[]> consumerIt;

    final int iterations;

    public SimpleTestKafkaConsumer(String topic, String zookeeperConnect,
                                   int iterations) {

        this.iterations = iterations;

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("group.id", "test_consumer_1");
        props.put("auto.offset.reset", "smallest");

        consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, Integer.valueOf(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        consumerIt = stream.iterator();
    }

    public void shutdown() {
        consumer.shutdown();
    }

    @Override
    public Iterator<byte[]> iterator() {

        Iterator<byte[]> it = new Iterator<byte[]>() {

            int counter = 0;

            @Override
            public boolean hasNext() {
                if (counter < iterations) {
                    return consumerIt.hasNext();
                } else {
                    return false;
                }
            }

            @Override
            public byte[] next() {
                counter++;
                return consumerIt.next().message();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return it;
    }
}