package org.schedoscope.export.bigquery.outputformat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.bigquery.outputschema.PartitioningScheme;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.convertSchemaToTableDefinition;
import static org.schedoscope.export.utils.BigQueryUtils.*;

public class BigQueryOutputFormat<K, V extends Map<String, Object>> extends OutputFormat<K, V> {

    private static Configuration configuration;
    private static String project;
    private static String database;
    private static String table;
    private static String usedHCatFilter;
    private static HCatSchema hcatSchema;
    private static String gcpKey;
    private static String tableNamePostfix;
    private static int commitSize;
    private static BigQuery bigQueryService;


    public static void setOutput(Configuration conf, String project, String gcpKey, String database, String table, HCatSchema hcatSchema, String usedHCatFilter, String tableNamePostfix, int commitSize) throws IOException {
        configuration = conf;
        BigQueryOutputFormat.project = project;
        BigQueryOutputFormat.database = database;
        BigQueryOutputFormat.table = table;
        BigQueryOutputFormat.usedHCatFilter = usedHCatFilter;
        BigQueryOutputFormat.hcatSchema = hcatSchema;
        BigQueryOutputFormat.gcpKey = gcpKey;
        BigQueryOutputFormat.commitSize = commitSize;
        bigQueryService = bigQueryService(gcpKey);
    }

    public class BiqQueryRecordWriter extends RecordWriter<K, V> {

        private TableId tableId;
        private int commitSize;
        private Map<String, Object>[] batch;
        private int elementsInBatch = 0;
        private BigQuery bigQueryService;

        @Override
        public void write(K key, V value) {
            batch[elementsInBatch] = value;
            elementsInBatch++;

            if (elementsInBatch == commitSize) {
                retry(3, () -> insertIntoTable(bigQueryService, tableId, batch));

                elementsInBatch = 0;
            }

        }

        @Override
        public void close(TaskAttemptContext context) {

            if (elementsInBatch > 0) {
                retry(3, () -> insertIntoTable(bigQueryService, tableId, Arrays.copyOf(batch, elementsInBatch)));
            }

        }

        public BiqQueryRecordWriter(BigQuery bigQueryService, TableId tableId, int commitSize) {
            this.bigQueryService = bigQueryService;
            this.tableId = tableId;
            this.commitSize = commitSize;
            this.batch = new Map[commitSize];
        }
    }


    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        TableDefinition outputSchema = convertSchemaToTableDefinition(hcatSchema, new PartitioningScheme());

        String tmpOutputTable = table
                + (tableNamePostfix != null ? "_" + tableNamePostfix : "")
                + "_" + context.getTaskAttemptID().getTaskID().getId();

        TableId tmpTableId = project == null ? TableId.of(database, tmpOutputTable) : TableId.of(project, database, tmpOutputTable);

        retry(3, () -> {
            dropTable(bigQueryService, tmpTableId);
            createTable(bigQueryService, tmpTableId, outputSchema);
        });

        return new BiqQueryRecordWriter(bigQueryService, tmpTableId, commitSize);

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
