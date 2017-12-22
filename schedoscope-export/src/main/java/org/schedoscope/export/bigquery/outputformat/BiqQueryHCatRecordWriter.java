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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

import static org.schedoscope.export.bigquery.outputschema.HCatRecordToBigQueryMapConvertor.convertHCatRecordToBigQueryMap;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.USED_FILTER_FIELD_NAME;
import static org.schedoscope.export.utils.CloudStorageUtils.createBlobIfNotExists;

/**
 * A writer for the BigQuery output format that transforms HCatRecords to JSON, and stores them to a cloud storage bucket.
 *
 * @param <K> ingored
 * @param <V> a subtype of HCatRecord
 */
public class BiqQueryHCatRecordWriter<K, V extends HCatRecord> extends RecordWriter<K, V> {

    private HCatSchema hcatSchema;
    private Storage storageService;

    private String usedHCatFilter;
    private String bucket;
    private String blobName;
    private String region;

    private WritableByteChannel channel;

    private ObjectMapper jsonFactory = new ObjectMapper();

    @Override
    public void write(K key, V value) throws IOException {
        if (channel == null) {
            channel = createBlobIfNotExists(storageService, bucket, blobName, region).writer();
        }

        Map<String, Object> recordMap = convertHCatRecordToBigQueryMap(hcatSchema, value);
        recordMap.put(USED_FILTER_FIELD_NAME, this.usedHCatFilter);

        String output = jsonFactory.writeValueAsString(recordMap) + "\n";

        channel.write(ByteBuffer.wrap(output.getBytes("UTF-8")));
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
     * @param hcatSchema     the HCat schema to which the records conform.
     * @param usedHCatFilter the HCat filter expression that was used to read the HCat records passing through the writer.
     */
    public BiqQueryHCatRecordWriter(Storage storageService, String bucket, String blobName, String region, HCatSchema hcatSchema, String usedHCatFilter) {
        this.storageService = storageService;
        this.bucket = bucket;
        this.blobName = blobName;
        this.region = region;
        this.hcatSchema = hcatSchema;
        this.usedHCatFilter = usedHCatFilter;
    }

}
