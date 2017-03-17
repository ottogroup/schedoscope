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

package org.schedoscope.export.ftp.outputformat;

/**
 * The Json Record Writer is used to write the records as JSON to a file.
 */

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonRecordWriter<K, V> extends RecordWriter<K, V> {

    private static final String NEWLINE = "\n";

    private DataOutputStream out;

    /**
     * The constructor to initialize the Json Record Writer.
     *
     * @param out A data output stream.
     */
    public JsonRecordWriter(DataOutputStream out) {

        this.out = out;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(K key, V value) throws IOException {

        AvroValue<GenericRecord> avroValue = (AvroValue<GenericRecord>) value;
        out.write(avroValue.datum().toString().getBytes(StandardCharsets.UTF_8));
        out.write(NEWLINE.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {

        out.close();
    }
}
