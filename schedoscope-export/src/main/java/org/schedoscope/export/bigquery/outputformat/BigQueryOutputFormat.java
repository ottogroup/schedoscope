package org.schedoscope.export.bigquery.outputformat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.bigquery.outputschema.PartitioningScheme;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.*;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.convertSchemaToTableDefinition;
import static org.schedoscope.export.bigquery.outputschema.PartitioningScheme.DAILY;
import static org.schedoscope.export.bigquery.outputschema.PartitioningScheme.NONE;
import static org.schedoscope.export.utils.BigQueryUtils.*;

public class BigQueryOutputFormat<K, V extends HCatRecord> extends OutputFormat<K, V> {


    public static void prepareBigQueryTable(Configuration conf) throws IOException {

        PartitioningScheme partitioning = getBigQueryTablePartitionDate(conf) != null ? DAILY : NONE;

        TableDefinition outputSchema = convertSchemaToTableDefinition(getBigQueryHCatSchema(conf), partitioning);

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));

        retry(3, () -> {
            createTable(bigQueryService, getBigQueryTableId(conf), outputSchema);
        });

    }

    public static void commit(Configuration conf) throws IOException {

    }

    public static void rollback(Configuration conf) throws IOException {

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));
        TableId tableId = getBigQueryTableId(conf, true);

        retry(3, () -> {
            dropTable(bigQueryService, tableId);
        });

    }

    public class BiqQueryHCatRecordWriter extends RecordWriter<K, V> {

        private HCatSchema hcatSchema;
        private BigQuery bigQueryService;
        private String usedHCatFilter;
        private String bucket;
        private String blobName;

        private BlobId blobId;
        private WritableByteChannel channel;


        @Override
        public void write(K key, V value) throws IOException {

            if (blobId == null && channel == null) {

            }




        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            if (channel != null)
                channel.close();
        }

        public BiqQueryHCatRecordWriter(BigQuery bigQueryService, String bucket, String blobName, HCatSchema hcatSchema, String usedHCatFilter) {
            this.bigQueryService = bigQueryService;
            this.bucket = bucket;
            this.blobName = blobName;
            this.hcatSchema = hcatSchema;
            this.usedHCatFilter = usedHCatFilter;
        }

    }


    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));

        return new BiqQueryHCatRecordWriter(bigQueryService, getBigQueryExportStorageBucket(conf), getBigQueryFullTableName(conf, true) + "/" + context.getTaskAttemptID().toString(), getBigQueryHCatSchema(conf), getBigQueryUsedHcatFilter(conf));
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
