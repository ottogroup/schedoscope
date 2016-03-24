package org.schedoscope.export.kafka.outputformat;

/**
 * An enum representing the different
 * compression codecs Kafka can use (gzip / snappy / none).
 */
public enum CompressionCodec {
    none {
        @Override
        public String toString() {
            return "none";
        }
    },
    snappy {
        @Override
        public String toString() {
            return "snappy";
        }
    },
    gzip {
        @Override
        public String toString() {
            return "gzip";
        }
    }
}
