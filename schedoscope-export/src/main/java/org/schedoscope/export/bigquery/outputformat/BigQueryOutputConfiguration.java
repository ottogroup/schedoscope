package org.schedoscope.export.bigquery.outputformat;

import com.google.cloud.bigquery.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.*;
import java.util.Base64;

public class BigQueryOutputConfiguration {

    public static final String BIGQUERY_PROJECT = "bigquery.project";
    public static final String BIGQUERY_DATASET = "bigquery.dataset";
    public static final String BIGQUERY_TABLE = "bigquery.table";
    public static final String BIGQUERY_TABLE_PARTITION_DATE = "bigquery.tablePartitionDate";
    public static final String BIGQUERY_USED_HCAT_FILTER = "bigquery.usedHCatFilter";
    public static final String BIGQUERY_HCAT_SCHEMA = "bigquery.hcatSchema";
    public static final String BIGQUERY_NO_OF_WORKERS = "bigquery.noOfPartitions";
    public static final String BIGQUERY_GCP_KEY = "bigquery.gcpKey";
    public static final String BIGQUERY_EXPORT_STORAGE_BUCKET = "bigquery.exportStorageBucket";


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

    public static int getBigQueryNoOfWorkers(Configuration conf) {
        return Integer.parseInt(conf.get(BIGQUERY_NO_OF_WORKERS));
    }

    public static String getBigQueryExportStorageBucket(Configuration conf) {
        return conf.get(BIGQUERY_EXPORT_STORAGE_BUCKET);
    }

    public static HCatSchema getBigQueryHCatSchema(Configuration conf) throws IOException {
        try {
            return deserializeHCatSchema(conf.get(BIGQUERY_HCAT_SCHEMA));
        } catch (ClassNotFoundException e) {
            throw new IOException("Error while deserializing HCatSchema", e);
        }
    }

    public static TableId getBigQueryTableId(Configuration conf, boolean includingPartition) {
        String bigQueryTableName = getBigQueryTable(conf) + (includingPartition && getBigQueryTablePartitionDate(conf) != null ? "$" + getBigQueryTablePartitionDate(conf) : "");

        return getBigQueryProject(conf) == null ? TableId.of(getBigQueryDataset(conf), bigQueryTableName) : TableId.of(getBigQueryProject(conf), getBigQueryDataset(conf), bigQueryTableName);
    }

    public static TableId getBigQueryTableId(Configuration conf) {
        return getBigQueryTableId(conf, false);
    }

    public static String getBigQueryFullTableName(Configuration conf, boolean includingPartition) {
        TableId tableId = getBigQueryTableId(conf, includingPartition);

        return (tableId.getProject() != null ? tableId.getProject() + "." : "")
                + tableId.getDataset() + "." + tableId.getTable();
    }

    public static String getBigQueryFullTableName(Configuration conf) {
        return getBigQueryFullTableName(conf, false);
    }

    public static Configuration configureBigQueryOutput(Configuration currentConf, String project, String gcpKey, String database, String table, String tablePartitionDate, String usedHCatFilter, HCatSchema hcatSchema, String exportStorageBucket, int commitSize, int noOfPartitions) throws IOException {

        if (project != null)
            currentConf.set(BIGQUERY_PROJECT, project);

        if (gcpKey != null)
            currentConf.set(BIGQUERY_GCP_KEY, gcpKey);

        currentConf.set(BIGQUERY_DATASET, database);
        currentConf.set(BIGQUERY_TABLE, table);

        if (tablePartitionDate != null)
            currentConf.set(BIGQUERY_TABLE_PARTITION_DATE, tablePartitionDate);

        if (usedHCatFilter != null)
            currentConf.set(BIGQUERY_USED_HCAT_FILTER, usedHCatFilter);

        currentConf.set(BIGQUERY_EXPORT_STORAGE_BUCKET, exportStorageBucket);

        currentConf.set(BIGQUERY_NO_OF_WORKERS, String.valueOf(noOfPartitions));
        currentConf.set(BIGQUERY_HCAT_SCHEMA, serializeHCatSchema(hcatSchema));

        return currentConf;

    }

}
