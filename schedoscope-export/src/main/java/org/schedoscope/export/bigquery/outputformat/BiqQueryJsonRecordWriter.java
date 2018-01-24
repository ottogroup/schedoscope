/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.export.bigquery.outputformat;

import com.google.cloud.storage.Storage;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static org.schedoscope.export.utils.CloudStorageUtils.createBlobIfNotExists;

/**
 * A writer for the BigQuery output format that stores JSON-formatted records for BigQuery to a cloud storage bucket.
 *
 * @param <K> ignored
 */
public class BiqQueryJsonRecordWriter<K> extends RecordWriter<K, Text> {

    private Storage storageService;
    private String bucket;
    private String blobName;
    private String region;

    private WritableByteChannel channel;


    @Override
    public void write(K key, Text value) throws IOException {
        if (channel == null) {
            channel = createBlobIfNotExists(storageService, bucket, blobName, region).writer();
        }

        channel.write(ByteBuffer.wrap(value.toString().getBytes("UTF-8")));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        if (channel != null)
            channel.close();
    }

    /**
     * Constructor for the record writer.
     *
     * @param storageService reference to Google Cloud Storage web service
     * @param bucket         the bucket to write data to. The bucket gets created if it does not exist
     * @param blobName       the name of the blob to write data to
     * @param region         the storage region where the bucket is created if created.
     */
    public BiqQueryJsonRecordWriter(Storage storageService, String bucket, String blobName, String region) {
        this.storageService = storageService;
        this.bucket = bucket;
        this.blobName = blobName;
        this.region = region;
    }

}
