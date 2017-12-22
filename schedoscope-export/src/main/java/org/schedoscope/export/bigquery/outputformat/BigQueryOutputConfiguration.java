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

import com.google.cloud.bigquery.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.*;
import java.util.Base64;

/**
 * Maintains the Hadoop configuration keys and values required for BigQueryOutputFormat to work.
 * The configurations are distributed to the various mappers using Hadoop's Configuration object mechanism.
 */
public class BigQueryOutputConfiguration {

    public static final String BIGQUERY_PROJECT = "bigquery.project";
    public static final String BIGQUERY_DATABASE = "bigquery.dataset";
    public static final String BIGQUERY_TABLE = "bigquery.table";
    public static final String BIGQUERY_TABLE_PARTITION_DATE = "bigquery.tablePartitionDate";
    public static final String BIGQUERY_TABLE_POSTFIX = "bigquery.tablePartitionPostfix";
    public static final String BIGQUERY_DATASET_LOCATION = "bigquery.datasetLocation";
    public static final String BIGQUERY_USED_HCAT_FILTER = "bigquery.usedHCatFilter";
    public static final String BIGQUERY_HCAT_SCHEMA = "bigquery.hcatSchema";
    public static final String BIGQUERY_NO_OF_WORKERS = "bigquery.noOfPartitions";
    public static final String BIGQUERY_GCP_KEY = "bigquery.gcpKey";
    public static final String BIGQUERY_EXPORT_STORAGE_BUCKET = "bigquery.exportStorageBucket";
    public static final String BIGQUERY_EXPORT_STORAGE_FOLDER_PREFIX = "bigquery.exportStorageFolderPrefix";
    public static final String BIGQUERY_EXPORT_STORAGE_REGION = "bigquery.exportStorageRegion";
    public static final String BIGQUERY_PROXY_HOST = "bigquery.proxyHost";
    public static final String BIGQUERY_PROXY_PORT = "bigquery.proxyPort";

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

    /**
     * Return the name of the GCP project to use for BigQuery access from the Hadoop configuration. If not set,
     * this will default to the default GCP project of the current user.
     *
     * @param conf the Hadoop configuration object
     * @return the GCP project ID
     */
    public static String getBigQueryProject(Configuration conf) {
        return conf.get(BIGQUERY_PROJECT);
    }

    /**
     * Return the GCP key to use for BigQuery access from the Hadoop configuration
     *
     * @param conf the Hadoop configuration object
     * @return the GCP key
     */
    public static String getBigQueryGcpKey(Configuration conf) {
        return conf.get(BIGQUERY_GCP_KEY);
    }

    /**
     * Return the name of database of the Hive table to export from the Hadoop configuration. This will become the dataset
     * in BigQuery.
     *
     * @param conf the Hadoop configuration object
     * @return the dataset
     */
    public static String getBigQueryDatabase(Configuration conf) {
        return conf.get(BIGQUERY_DATABASE);
    }

    /**
     * Return the storage location of the BigQuery dataset from the Hadoop configuration.
     * If not set, this will default to EU.
     *
     * @param conf the Hadoop configuration object
     * @return the dataset
     */
    public static String getBigQueryDatasetLocation(Configuration conf) {
        return conf.get(BIGQUERY_DATASET_LOCATION);
    }

    /**
     * Return the name of the Hive table to export from the Hadoop configuration. The BigQuery table name will be identical,
     * unless augmented with a postfix.
     *
     * @param conf the Hadoop configuration object
     * @return the table name
     */
    public static String getBigQueryTable(Configuration conf) {
        return conf.get(BIGQUERY_TABLE);
    }

    /**
     * Return the postfix to append to the BigQuery table name to export a given Hive table to from the Hadoop configuration.
     *
     * @param conf the Hadoop configuration object
     * @return the table name postfix
     */
    public static String getBigqueryTablePostfix(Configuration conf) {
        return conf.get(BIGQUERY_TABLE_POSTFIX);
    }

    /**
     * Return the HCAT filter expression to apply to the Hive table being exported to BigQuery for partition selection
     * from the Hadoop configuration.
     *
     * @param conf the Hadoop configuration object
     * @return the filter expression
     */
    public static String getBigQueryUsedHcatFilter(Configuration conf) {
        return conf.get(BIGQUERY_USED_HCAT_FILTER);
    }

    /**
     * Return the partition date (YYYYMMDD) of the BigQuery table to which to export the Hive table to from the Hadoop
     * configuration. If not set, the table will not be partitioned.
     *
     * @param conf the Hadoop configuration object
     * @return the partition date.
     */
    public static String getBigQueryTablePartitionDate(Configuration conf) {
        return conf.get(BIGQUERY_TABLE_PARTITION_DATE);
    }

    /**
     * Return the number of parallel workers (mappers) exporting the Hive table data to BigQuery from the Hadoop
     * configuration.
     *
     * @param conf the Hadoop configuration object
     * @return the number of workers.
     */
    public static int getBigQueryNoOfWorkers(Configuration conf) {
        return Integer.parseInt(conf.get(BIGQUERY_NO_OF_WORKERS));
    }

    /**
     * Return the name of the Google Cloud Storage bucket used for temporal storage of exported Hive table data from the
     * Hadoop configuration.
     *
     * @param conf the Hadoop configuration object
     * @return the GCP storage bucket.
     */
    public static String getBigQueryExportStorageBucket(Configuration conf) {
        return conf.get(BIGQUERY_EXPORT_STORAGE_BUCKET);
    }

    /**
     * Return the folder prefix to prepend to the temporal storage files in the GCP storage bucket from the Hadoop
     * configuration. Defaults to ""
     *
     * @param conf the Hadoop configuration object
     * @return the prefix.
     */
    public static String getBigqueryExportStorageFolderPrefix(Configuration conf) {
        return conf.get(BIGQUERY_EXPORT_STORAGE_FOLDER_PREFIX);
    }

    /**
     * Return the region in which temporal Hive table export data should be kept in GCP Cloud storage. Defaults
     * to "europe-west3"
     *
     * @param conf the Hadoop configuration object
     * @return the storage region
     */
    public static String getBigqueryExportStorageRegion(Configuration conf) {
        return conf.get(BIGQUERY_EXPORT_STORAGE_REGION);
    }

    /**
     * Return the folder within the GCP storage bucket temporal data exported from Hive should be exported to.
     *
     * @param conf the Hadoop configuration object
     * @return the folder
     */
    public static String getBigQueryExportStorageFolder(Configuration conf) {
        return (getBigqueryExportStorageFolderPrefix(conf) != null && !getBigqueryExportStorageFolderPrefix(conf).isEmpty()) ? getBigqueryExportStorageFolderPrefix(conf) + "/" + getBigQueryFullTableName(conf, true) : getBigQueryFullTableName(conf, true);
    }

    /**
     * Return the HCat schema of the Hive table being exported to BigQuery
     *
     * @param conf the Hadoop configuration object
     * @return the HCat schema
     * @throws IOException
     */
    public static HCatSchema getBigQueryHCatSchema(Configuration conf) throws IOException {
        try {
            return deserializeHCatSchema(conf.get(BIGQUERY_HCAT_SCHEMA));
        } catch (ClassNotFoundException e) {
            throw new IOException("Error while deserializing HCatSchema", e);
        }
    }

    /**
     * Return a table ID address the BigQuery export table.
     *
     * @param conf               the Hadoop configuration
     * @param includingPartition should the table name include the partition selector in case of a partitioned target.
     * @return the table ID
     */
    public static TableId getBigQueryTableId(Configuration conf, boolean includingPartition) {
        String bigQueryTableName = getBigQueryTable(conf) +
                (getBigqueryTablePostfix(conf) != null ? "_" + getBigqueryTablePostfix(conf) : "") +
                (includingPartition && getBigQueryTablePartitionDate(conf) != null ? "$" + getBigQueryTablePartitionDate(conf) : "");

        return getBigQueryProject(conf) == null ? TableId.of(getBigQueryDatabase(conf), bigQueryTableName) : TableId.of(getBigQueryProject(conf), getBigQueryDatabase(conf), bigQueryTableName);
    }

    /**
     * Return a table ID address the BigQuery export table with no partition selector.
     *
     * @param conf the Hadoop configuration
     * @return the table ID
     */
    public static TableId getBigQueryTableId(Configuration conf) {
        return getBigQueryTableId(conf, false);
    }

    /**
     * Return the fully qualified BigQuery table name serving as the Hive table export target.
     *
     * @param conf               the Hadoop configuration
     * @param includingPartition should the table name include the partition selector in case of a partitioned target.
     * @return the table name
     */
    public static String getBigQueryFullTableName(Configuration conf, boolean includingPartition) {
        TableId tableId = getBigQueryTableId(conf, includingPartition);

        return (tableId.getProject() != null ? tableId.getProject() + "." : "")
                + tableId.getDataset() + "." + tableId.getTable();
    }

    /**
     * Return the fully qualified BigQuery table name serving as the Hive table export target without partition designation.
     *
     * @param conf the Hadoop configuration
     * @return the table name
     */
    public static String getBigQueryFullTableName(Configuration conf) {
        return getBigQueryFullTableName(conf, false);
    }

    /**
     * Return the configured host running the HTTPS proxy to route proxy requests through.
     *
     * @param conf the Hadoop configuration
     * @return the HTTPS proxy host
     */
    public static String getBigQueryProxyHost(Configuration conf) {
        return conf.get(BIGQUERY_PROXY_HOST);
    }

    /**
     * Return the configured port of the HTTPS proxy to route proxy requests through.
     *
     * @param conf the Hadoop configuration
     * @return the HTTPS proxy port
     */
    public static String getBigQueryProxyPort(Configuration conf) {
        return conf.get(BIGQUERY_PROXY_PORT);
    }


    /**
     * Augment a given Hadoop configuration with additional parameters required for BigQuery Hive table export.
     *
     * @param currentConf               the Hadoop configuration to augment
     * @param project                   the name of the GCP project to use for BigQuery access from the Hadoop
     *                                  configuration. If null, this will default to the default GCP project of the
     *                                  current user.
     * @param gcpKey                    the GCP key to use for GCP access. If null, the default key according to the GCP
     *                                  authentication mechanism will be used.
     * @param database                  the name of the database of the Hive table being exported.
     * @param table                     the name of the Hive table being exported.
     * @param tablePostfix              the postfix to append to the table name with _. Useful to model non-temporal
     *                                  Hive partition values.
     * @param dataLocation              the storage location where the resulting BigQuery table should be stored. Default to EU
     * @param tablePartitionDate        if set, the BigQuery table will be partitioned by day. The Hive table data will
     *                                  be exported into the given partition. If null, the resulting table is not partitioned.
     * @param usedHCatFilter            if set with a HCat table filter, only Hive data matching that filter will be
     *                                  exported. This is used to export only a partition of a given table. Pass null if
     *                                  you do not want filtering.
     * @param hcatSchema                the HCat schema of the Hive table that is being exported.
     * @param exportStorageBucket       the GCP Cloud Storage bucket to use for storing temporal data during export.
     * @param exportStorageFolderPrefix a path prefix to append to the storage folder for temporal data in the GCP
     *                                  storage bucket. "" if null is given.
     * @param exportStorageRegion       the region where to store the GCP bucket. If null, the default region is chosen
     *                                  to be "europe-west3"
     * @param noOfPartitions            the parallelism with which to perform the export. Defaults to 1 in case you pass null.
     * @param proxyHost                 the host running the HTTPS proxy to route traffic through. Set to null if no proxy is to be used.
     * @param proxyPort                 the port of the HTTPS proxy to route traffic through. Set to null if no proxy is to be used.
     * @return
     * @throws IOException
     */
    public static Configuration configureBigQueryOutputFormat(Configuration currentConf, String project, String gcpKey, String database, String table, String tablePostfix, String dataLocation, String tablePartitionDate, String usedHCatFilter, HCatSchema hcatSchema, String exportStorageBucket, String exportStorageFolderPrefix, String exportStorageRegion, Integer noOfPartitions, String proxyHost, String proxyPort) throws IOException {

        if (project != null)
            currentConf.set(BIGQUERY_PROJECT, project);

        if (gcpKey != null)
            currentConf.set(BIGQUERY_GCP_KEY, gcpKey);

        currentConf.set(BIGQUERY_DATABASE, database);
        currentConf.set(BIGQUERY_TABLE, table);

        if (tablePostfix != null)
            currentConf.set(BIGQUERY_TABLE_POSTFIX, tablePostfix);

        if (dataLocation != null)
            currentConf.set(BIGQUERY_DATASET_LOCATION, dataLocation);
        else
            currentConf.set(BIGQUERY_DATASET_LOCATION, "EU");

        if (tablePartitionDate != null)
            currentConf.set(BIGQUERY_TABLE_PARTITION_DATE, tablePartitionDate);

        if (usedHCatFilter != null)
            currentConf.set(BIGQUERY_USED_HCAT_FILTER, usedHCatFilter);

        currentConf.set(BIGQUERY_EXPORT_STORAGE_BUCKET, exportStorageBucket);

        if (exportStorageFolderPrefix != null)
            currentConf.set(BIGQUERY_EXPORT_STORAGE_FOLDER_PREFIX, exportStorageFolderPrefix);
        else
            currentConf.set(BIGQUERY_EXPORT_STORAGE_FOLDER_PREFIX, "");

        if (exportStorageRegion != null)
            currentConf.set(BIGQUERY_EXPORT_STORAGE_REGION, exportStorageRegion);
        else
            currentConf.set(BIGQUERY_EXPORT_STORAGE_REGION, "europe-west3");


        currentConf.set(BIGQUERY_NO_OF_WORKERS, String.valueOf(noOfPartitions != null && noOfPartitions > 0 ? noOfPartitions : 1));
        currentConf.set(BIGQUERY_HCAT_SCHEMA, serializeHCatSchema(hcatSchema));

        if (proxyHost != null && proxyPort != null) {
            currentConf.set(BIGQUERY_PROXY_HOST, proxyHost);
            currentConf.set(BIGQUERY_PROXY_PORT, proxyPort);
        }

        return currentConf;

    }

}
