package org.schedoscope.export.bigquery.outputformat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.bigquery.BigQueryUtils;
import org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter;
import org.schedoscope.export.bigquery.outputschema.PartitioningScheme;

import java.io.IOException;

public class BigQueryOutputFormat<K, V> extends OutputFormat<K, V> {

    private static Configuration configuration;
    private static String project;
    private static String database;
    private static String table;
    private static String usedHCatFilter;
    private static HCatSchema hcatSchema;
    private static String gcpKey;
    private static String tableNamePostfix;
    private static HCatSchemaToBigQuerySchemaConverter HCatSchemaToBigQuerySchemaConverter = new HCatSchemaToBigQuerySchemaConverter();
    private static BigQueryUtils execute;
    private static BigQuery bigQueryService;


    public static void setOutput(Configuration conf, String project, String gcpKey, String database, String table, HCatSchema hcatSchema, String usedHCatFilter, String tableNamePostfix, BigQueryUtils bigQueryUtils) throws IOException {
        configuration = conf;
        BigQueryOutputFormat.project = project;
        BigQueryOutputFormat.database = database;
        BigQueryOutputFormat.table = table;
        BigQueryOutputFormat.usedHCatFilter = usedHCatFilter;
        BigQueryOutputFormat.hcatSchema = hcatSchema;
        BigQueryOutputFormat.gcpKey = gcpKey;
        execute = bigQueryUtils;
        bigQueryService = execute.bigQueryService(gcpKey);
    }

    public static void setOutput(Configuration conf, String gcpKey, String database, String table, HCatSchema hcatSchema, String usedHCatFilter, String tableNamePostfix, BigQueryUtils bigQueryUtils) {
        setOutput(conf, gcpKey, database, table, hcatSchema, usedHCatFilter, tableNamePostfix, bigQueryUtils);
    }

    public static void setOutput(Configuration conf, String project, String gcpKey, String database, String table, HCatSchema hcatSchema, String usedHCatFilter, String tableNamePostfix) throws IOException {
        setOutput(conf, project, gcpKey, database, table, hcatSchema, usedHCatFilter, tableNamePostfix, new BigQueryUtils());
    }

    public static void setOutput(Configuration conf, String gcpKey, String database, String table, HCatSchema hcatSchema, String usedHCatFilter, String tableNamePostfix) throws IOException {
        setOutput(conf, null, gcpKey, database, table, hcatSchema, usedHCatFilter, tableNamePostfix);
    }


    public class BiqQueryRecordWriter extends RecordWriter<K,V> {

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {

        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }



    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        TableDefinition outputSchema = HCatSchemaToBigQuerySchemaConverter.convertSchemaToTableDefinition(hcatSchema, new PartitioningScheme());

        String tmpOutputTable = table
                + (tableNamePostfix != null ? "_" + tableNamePostfix : "")
                + "_" + context.getTaskAttemptID().getTaskID().getId();


        execute.dropTable(bigQueryService, project, database, tmpOutputTable);
        execute.createTable(bigQueryService, project, database, tmpOutputTable, outputSchema);

        return null;

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
