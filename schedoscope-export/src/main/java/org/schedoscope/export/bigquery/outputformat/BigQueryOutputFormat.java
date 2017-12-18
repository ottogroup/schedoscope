package org.schedoscope.export.bigquery.outputformat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.bigquery.outputschema.PartitioningScheme;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.*;
import static org.schedoscope.export.bigquery.outputschema.HCatRecordToBigQueryMapConvertor.convertHCatRecordToBigQueryMap;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.USED_FILTER_FIELD_NAME;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.convertSchemaToTableDefinition;
import static org.schedoscope.export.bigquery.outputschema.PartitioningScheme.DAILY;
import static org.schedoscope.export.bigquery.outputschema.PartitioningScheme.NONE;
import static org.schedoscope.export.utils.BigQueryUtils.*;
import static org.schedoscope.export.utils.CloudStorageUtils.*;

public class BigQueryOutputFormat<K, V extends HCatRecord> extends OutputFormat<K, V> {


    public static void prepareBigQueryTable(Configuration conf) throws IOException {

        PartitioningScheme partitioning = getBigQueryTablePartitionDate(conf) != null ? DAILY : NONE;

        TableDefinition outputSchema = convertSchemaToTableDefinition(getBigQueryHCatSchema(conf), partitioning);

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));

        retry(3, () -> {
            createTable(bigQueryService, getBigQueryTableId(conf), outputSchema);
        });

    }

    public static void commit(Configuration conf) throws IOException, TimeoutException, InterruptedException {

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));
        Storage storageService = storageService(getBigQueryGcpKey(conf));

        List<String> blobsToLoad = listBlobs(storageService, getBigQueryExportStorageBucket(conf), getBigQueryExportStorageFolder(conf));
        TableId tableId = getBigQueryTableId(conf, true);

        retry(3, () -> loadTable(bigQueryService, tableId, blobsToLoad));

        try {
            rollbackStorage(conf);
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    public static void rollback(Configuration conf) {

        try {
            rollbackBigQuery(conf);
        } catch (Throwable t) {
            t.printStackTrace();
        }

        try {
            rollbackStorage(conf);
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    private static void rollbackBigQuery(Configuration conf) throws IOException {
        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));
        TableId tableId = getBigQueryTableId(conf, true);

        retry(3, () -> {
            dropTable(bigQueryService, tableId);
        });
    }

    private static void rollbackStorage(Configuration conf) throws IOException {
        Storage storageService = storageService(getBigQueryGcpKey(conf));
        String bucket = getBigQueryExportStorageBucket(conf);
        String blobPrefix = getBigQueryExportStorageFolder(conf);

        retry(3, () -> {
            deleteBlob(storageService, bucket, blobPrefix);
        });
    }

    public class BiqQueryHCatRecordWriter extends RecordWriter<K, V> {

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

        public BiqQueryHCatRecordWriter(Storage storageService, String bucket, String blobName, String region, HCatSchema hcatSchema, String usedHCatFilter) {
            this.storageService = storageService;
            this.bucket = bucket;
            this.blobName = blobName;
            this.region = region;
            this.hcatSchema = hcatSchema;
            this.usedHCatFilter = usedHCatFilter;
        }

    }


    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        Storage storageService = storageService(getBigQueryGcpKey(conf));

        return new BiqQueryHCatRecordWriter(
                storageService,
                getBigQueryExportStorageBucket(conf),
                getBigQueryExportStorageFolder(conf) + "/" + context.getTaskAttemptID().toString(),
                getBigqueryExportStorageRegion(conf),
                getBigQueryHCatSchema(conf),
                getBigQueryUsedHcatFilter(conf));
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

}
