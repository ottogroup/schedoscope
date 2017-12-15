package org.schedoscope.export.bigquery.outputformat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.bigquery.outputschema.PartitioningScheme;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.*;
import static org.schedoscope.export.bigquery.outputschema.HCatRecordToBigQueryMapConvertor.convertHCatRecordToBigQueryMap;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.USED_FILTER_FIELD_NAME;
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

    public static void rollback(Configuration conf) throws IOException {

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));
        TableId tableId = getBigQueryTableId(conf, true);

        retry(3, () -> {
            dropTable(bigQueryService, tableId);
        });

    }

    public class BiqQueryHCatRecordWriter extends RecordWriter<K, V> {

        private TableId tableId;
        private int commitSize;
        private Map<String, Object>[] batch;
        private int elementsInBatch = 0;
        private HCatSchema hcatSchema;
        private BigQuery bigQueryService;
        private String usedHCatFilter;

        @Override
        public void write(K key, V value) throws IOException {

            try {

                Map<String, Object> bigQueryMap = convertHCatRecordToBigQueryMap(hcatSchema, value);
                if (usedHCatFilter != null)
                    bigQueryMap.put(USED_FILTER_FIELD_NAME, usedHCatFilter);

                batch[elementsInBatch] = bigQueryMap;
                elementsInBatch++;

                if (elementsInBatch == commitSize) {
                    retry(3, () -> insertIntoTable(bigQueryService, tableId, batch));
                    elementsInBatch = 0;
                }

            } catch (Throwable t) {
                throw new IOException("Exception encountered while writing HCatRecord to BigQuery", t);
            }

        }

        @Override
        public void close(TaskAttemptContext context) {

            if (elementsInBatch > 0) {
                retry(3, () -> insertIntoTable(bigQueryService, tableId, Arrays.copyOf(batch, elementsInBatch)));
            }

        }

        public BiqQueryHCatRecordWriter(BigQuery bigQueryService, TableId tableId, HCatSchema hcatSchema, String usedHCatFilter, int commitSize) {
            this.bigQueryService = bigQueryService;
            this.tableId = tableId;
            this.commitSize = commitSize;
            this.batch = new Map[commitSize];
            this.hcatSchema = hcatSchema;
            this.usedHCatFilter = usedHCatFilter;
        }

    }


    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {

        Configuration conf = context.getConfiguration();

        BigQuery bigQueryService = bigQueryService(getBigQueryGcpKey(conf));

        return new BiqQueryHCatRecordWriter(bigQueryService, getBigQueryTableId(conf, true), getBigQueryHCatSchema(conf), getBigQueryUsedHcatFilter(conf), getBigQueryCommitSize(conf));

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
