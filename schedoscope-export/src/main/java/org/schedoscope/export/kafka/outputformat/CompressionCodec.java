package org.schedoscope.export.kafka.outputformat;

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
