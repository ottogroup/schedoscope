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
package org.schedoscope.export.utils;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helpers for dealing with BigQuery.
 */
public class BigQueryUtils {

    final static private Random rnd = new Random();


    /**
     * Retrieve an instance of the BigQuery web service, authenticated using the default GCP authentication mechanism.
     *
     * @return the service instance.
     */
    static public BigQuery bigQueryService() {
        try {
            return bigQueryService(null, null);
        } catch (IOException e) {
            // not going to happen
            return null;
        }
    }

    /**
     * Retrieve an instance of the BigQuery web service, authenticated using the given GCP key and a given project ID.
     *
     * @param projectId the GCP project id to use.
     * @param gcpKey    the JSON formatted GCP key.
     * @return the service instance.
     * @throws IOException
     */
    static public BigQuery bigQueryService(String projectId, String gcpKey) throws IOException {
        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();

        if (projectId != null) {
            builder.setProjectId(projectId);
        }

        if (gcpKey != null) {
            GoogleCredentials credentials = GoogleCredentials
                    .fromStream(
                            new ByteArrayInputStream(Charset.forName("UTF-8").encode(gcpKey).array())
                    );

            builder.setCredentials(credentials);
        }


        return builder.build().getService();
    }

    /**
     * Helper to retry a lambda expression for a given number of times, until no exception is thrown.
     *
     * @param numberOfRetries the number of retries.
     * @param action          the lambda
     * @param <T>             the return type
     * @return the result of the lambda.
     */
    static public <T> T retry(int numberOfRetries, Supplier<T> action) {
        try {
            return action.get();
        } catch (Throwable t) {
            if (numberOfRetries > 0) {

                try {
                    Thread.currentThread().sleep(rnd.nextInt(2000));
                } catch (InterruptedException e) {
                }

                return retry(numberOfRetries - 1, action);
            } else
                throw t;
        }
    }

    /**
     * Helper to retry a lambda returning void for a given number of times, until no exception is thrown.
     *
     * @param numberOfRetries the number of retries.
     * @param action          the lambda
     */
    static public void retry(int numberOfRetries, Runnable action) {
        retry(numberOfRetries, () -> {
            action.run();
            return null;
        });
    }

    /**
     * Check whether a given dataset already exists.
     *
     * @param bigQueryService the BigQuery web service instance to use for the check.
     * @param project         the project owning the dataset or null, if the default project should be used.
     * @param dataset         the name of the dataset.
     * @return true iff the dataset already exists.
     */
    static public boolean existsDataset(BigQuery bigQueryService, String project, String dataset) {
        return bigQueryService.getDataset(project == null ? DatasetId.of(dataset) : DatasetId.of(project, dataset)) != null;
    }

    /**
     * Create a dataset, if it does not exist.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param project         the project to create the dataset in or null, if the default project should be used.
     * @param dataset         the name of the dataset to create.
     */
    static public void createDataset(BigQuery bigQueryService, String project, String dataset, String dataLocation) {
        if (!existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.create((project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).setLocation(dataLocation != null ? dataLocation : "EU").build());
        }
    }

    /**
     * Drop a dataset
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param project         the project to owning the dataset to drop or null, if the default project should be used.
     * @param dataset         the name of the dataset to drop.
     */
    static public void dropDataset(BigQuery bigQueryService, String project, String dataset) {
        if (existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.delete(
                    (project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build().getDatasetId(),
                    BigQuery.DatasetDeleteOption.deleteContents()
            );
        }
    }

    /**
     * Check whether a given table already exists.
     *
     * @param bigQueryService the BigQuery web service instance to use for the check.
     * @param tableId         the ID of the table to check.
     * @return true iff the table already exists.
     */
    static public boolean existsTable(BigQuery bigQueryService, TableId tableId) {
        Table table = bigQueryService.getTable(tableId);

        return table != null;
    }

    /**
     * Create a table, if it does not exist. If the dataset for the table does not exist, create that as well.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param tableId         the ID of the table to create.
     * @param tableDefinition the schema of the table to create.
     * @param dataLocation    the location where to create the table
     */
    static public void createTable(BigQuery bigQueryService, TableId tableId, TableDefinition tableDefinition, String dataLocation) {
        createDataset(bigQueryService, tableId.getProject(), tableId.getDataset(), dataLocation);

        if (!existsTable(bigQueryService, tableId))
            bigQueryService.create(TableInfo.of(tableId, tableDefinition));

    }

    /**
     * Create a table, if it does not exist. If the dataset for the table does not exist, create that as well.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param project         the project to create the table in or null, if the default project should be used.
     * @param dataset         the name of the dataset to create the table in.
     * @param table           the name of the table to create.
     * @param tableDefinition the schema of the table to create.
     * @param dataLocation    the location where to create the table
     */
    static public void createTable(BigQuery bigQueryService, String project, String dataset, String table, TableDefinition tableDefinition, String dataLocation) {
        createTable(bigQueryService, project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table), tableDefinition, dataLocation);
    }


    /**
     * Create a table, if it does not exist. If the dataset for the table does not exist, create that as well.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param tableInfo       the complete table info of the table to create.
     * @param dataLocation    the location where to create the table
     */
    static public void createTable(BigQuery bigQueryService, TableInfo tableInfo, String dataLocation) {
        createTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable(), tableInfo.getDefinition(), dataLocation);
    }


    /**
     * Drop a table.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param project         the project to drop the table from or null, if the default project should be used.
     * @param dataset         the name of the dataset to drop the table from.
     * @param table           the name of the table to drop.
     */
    static public void dropTable(BigQuery bigQueryService, String project, String dataset, String table) {
        bigQueryService.delete(project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table));
    }

    /**
     * Drop a table.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param tableId         the ID of the table to drop.
     */
    static public void dropTable(BigQuery bigQueryService, TableId tableId) {
        dropTable(bigQueryService, tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    /**
     * Load a table (partition) from a list of GCP Cloud Storage blobs. To load a partition, use a partition selector
     * in the table ID.
     *
     * @param bigQueryService         the BigQuery web service instance to use
     * @param table                   the ID of the table to load
     * @param cloudStoragePathsToData the list of gs:// URLs to the blobs to load into the table.
     */
    static public void loadTable(BigQuery bigQueryService, TableId table, List<String> cloudStoragePathsToData) {

        LoadJobConfiguration configuration = LoadJobConfiguration
                .newBuilder(table, cloudStoragePathsToData)
                .setFormatOptions(FormatOptions.json())
                .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                .build();

        //jobId could be used to reference the job later
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        JobInfo jobInfo = JobInfo.of(jobId, configuration);

        Job loadJob = bigQueryService.create(jobInfo);

        try {
            loadJob = loadJob.waitFor();
        } catch (InterruptedException e) {
            throw new BigQueryException(999, "Caught exception while inserting records into BigTable ", e);
        } catch (TimeoutException e) {
            throw new BigQueryException(999, "Caught exception while inserting records into BigTable ", e);
        }

        if (loadJob.getStatus().getError() != null) {
            throw new BigQueryException(999, "Could not insert some records into BigQuery table: " + loadJob.getStatus().getError());
        }
    }

    /**
     * Stream data into a table (partition). To stream into a partition, use a partition selector
     * in the table ID.
     *
     * @param bigQueryService the BigQuery web service instance to use
     * @param table           the ID of the table to load
     * @param rowsToInsert    a sequence of maps representing the BigQuery record to stream into the table
     */
    static public void insertIntoTable(BigQuery bigQueryService, TableId table, Map<String, Object>... rowsToInsert) {

        InsertAllRequest insertAllRequest = InsertAllRequest.newBuilder(table)
                .setRows(
                        Arrays.stream(rowsToInsert)
                                .map(InsertAllRequest.RowToInsert::of)
                                .collect(Collectors.toList())
                )
                .build();

        InsertAllResponse result = bigQueryService.insertAll(insertAllRequest);

        if (result.hasErrors()) {
            throw new BigQueryException(999, "Could not insert some records into BigQuery table: " + result.getInsertErrors().toString());
        }
    }

}
