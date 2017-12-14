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

import java.io.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import static org.schedoscope.export.bigquery.outputschema.HCatRecordToBigQueryMapConvertor.convertHCatRecordToBigQueryMap;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.USED_FILTER_FIELD_NAME;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.convertSchemaToTableDefinition;
import static org.schedoscope.export.bigquery.outputschema.PartitioningScheme.DAILY;
import static org.schedoscope.export.bigquery.outputschema.PartitioningScheme.NONE;
import static org.schedoscope.export.utils.BigQueryUtils.*;

public class BigQueryOutputFormat<K, V extends HCatRecord> extends OutputFormat<K, V> {


    public static final String BIGQUERY_PROJECT = "bigquery.project";
    public static final String BIGQUERY_DATASET = "bigquery.dataset";
    public static final String BIGQUERY_TABLE = "bigquery.table";
    public static final String BIGQUERY_TABLE_PARTITION_DATE = "bigquery.tablePartitionDate";
    public static final String BIGQUERY_USED_HCAT_FILTER = "bigquery.usedHCatFilter";
    public static final String BIGQUERY_HCAT_SCHEMA = "bigquery.hcatSchema";
    public static final String BIGQUERY_COMMIT_SIZE = "bigquery.commitSize";
    public static final String BIGQUERY_NO_OF_WORKERS = "bigquery.noOfPartitions";
    public static final String BIGQUERY_GCP_KEY = "bigquery.gcpKey";


    private static String serializeHCatSchema(HCatSchema schema) throws IOException {

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream serializer = new ObjectOutputStream(bytes);
        serializer.writeObject(schema);
        serializer.close();

        return Base64.getEncoder().encodeToString(bytes.toByteArray());

    }

    private static HCatSchema deserializeHCatSchema(String serializedSchema) throws IOException, ClassNotFoundException {
        byte[] bytes = Base64.getDecoder().decode(serializedSchema);
        ObjectInputStream deserializer = new ObjectInputStream(new ByteArrayInputStream(bytes));

        HCatSchema schema = (HCatSchema) deserializer.readObject();

        deserializer.close();

        return schema;

    }

    public static String getBigQueryProject(Configuration conf) {
        return conf.get(BIGQUERY_PROJECT);
    }

    public static String getBigQueryGcpKey(Configuration conf) {
        return conf.get(BIGQUERY_GCP_KEY);
    }

    public static String getBigQueryDataset(Configuration conf) {
        return conf.get(BIGQUERY_DATASET);
    }

    public static String getBigQueryTable(Configuration conf) {
        return conf.get(BIGQUERY_TABLE);
    }

    public static String getBigQueryUsedHcatFilter(Configuration conf) {
        return conf.get(BIGQUERY_USED_HCAT_FILTER);
    }

    public static String getBigQueryTablePartitionDate(Configuration conf) {
        return conf.get(BIGQUERY_TABLE_PARTITION_DATE);
    }

    public static int getBigQueryCommitSize(Configuration conf) {
        return Integer.parseInt(conf.get(BIGQUERY_COMMIT_SIZE));
    }

    public static int getBigQueryNoOfWorkers(Configuration conf) {
        return Integer.parseInt(conf.get(BIGQUERY_NO_OF_WORKERS));
    }

    public static HCatSchema getBigQueryHCatSchema(Configuration conf) throws IOException {
        try {
            return deserializeHCatSchema(conf.get(BIGQUERY_HCAT_SCHEMA));
        } catch (ClassNotFoundException e) {
            throw new IOException("Error while deserializing HCatSchema", e);
        }
    }

    public static Configuration configureBigQueryOutput(Configuration currentConf, String project, String gcpKey, String database, String table, String tablePartitionDate, String usedHCatFilter, HCatSchema hcatSchema, int commitSize, int noOfPartitions) throws IOException {

        currentConf.set(BIGQUERY_PROJECT, project);
        currentConf.set(BIGQUERY_GCP_KEY, gcpKey);
        currentConf.set(BIGQUERY_DATASET, database);
        currentConf.set(BIGQUERY_TABLE, table);
        currentConf.set(BIGQUERY_TABLE_PARTITION_DATE, tablePartitionDate);
        currentConf.set(BIGQUERY_USED_HCAT_FILTER, usedHCatFilter);
        currentConf.set(BIGQUERY_COMMIT_SIZE, String.valueOf(commitSize));
        currentConf.set(BIGQUERY_NO_OF_WORKERS, String.valueOf(noOfPartitions));
        currentConf.set(BIGQUERY_HCAT_SCHEMA, serializeHCatSchema(hcatSchema));

        return currentConf;

    }

    public static TableId getBigQueryTableId(Configuration conf, boolean includingPartition) {
        String bigQueryTableName = getBigQueryTable(conf) + (includingPartition && getBigQueryTablePartitionDate(conf) != null ? "$" + getBigQueryTablePartitionDate(conf) : "");

        return getBigQueryProject(conf) == null ? TableId.of(getBigQueryDataset(conf), bigQueryTableName) : TableId.of(getBigQueryProject(conf), getBigQueryDataset(conf), bigQueryTableName);
    }

    public static TableId getBigQueryTableId(Configuration conf) {
        return getBigQueryTableId(conf, false);
    }

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
