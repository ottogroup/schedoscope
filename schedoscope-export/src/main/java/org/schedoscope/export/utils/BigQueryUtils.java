package org.schedoscope.export.utils;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class BigQueryUtils {

    final static private Random rnd = new Random();

    static public BigQuery bigQueryService() {
        return BigQueryOptions.getDefaultInstance().getService();
    }


    static public BigQuery bigQueryService(String gcpKey) throws IOException {
        if (gcpKey == null)
            return bigQueryService();

        GoogleCredentials credentials = GoogleCredentials
                .fromStream(
                        new ByteArrayInputStream(Charset.forName("UTF-8").encode(gcpKey).array())
                );

        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }

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

    static public void retry(int numberOfRetries, Runnable action) {
        retry(numberOfRetries, () -> {
            action.run();
            return null;
        });
    }

    static public boolean existsDataset(BigQuery bigQueryService, String project, String dataset) {
        return bigQueryService.getDataset(project == null ? DatasetId.of(dataset) : DatasetId.of(project, dataset)) != null;
    }

    static public boolean existsDataset(BigQuery bigQueryService, String dataset) {
        return existsDataset(bigQueryService, null, dataset);
    }

    static public boolean existsDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        return existsDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    static public void createDataset(BigQuery bigQueryService, String project, String dataset) {
        if (!existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.create((project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build());
        }
    }

    static public void createDataset(BigQuery bigQueryService, String dataset) {
        createDataset(bigQueryService, null, dataset);
    }

    static public void createDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        createDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    static public void dropDataset(BigQuery bigQueryService, String project, String dataset) {
        if (existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.delete(
                    (project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build().getDatasetId(),
                    BigQuery.DatasetDeleteOption.deleteContents()
            );
        }
    }

    static public void dropDataset(BigQuery bigQueryService, String dataset) {
        dropDataset(bigQueryService, null, dataset);
    }

    static public void dropDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        dropDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    static public boolean existsTable(BigQuery bigQueryService, TableId tableId) {
        return bigQueryService.getTable(tableId) != null;
    }

    static public boolean existsTable(BigQuery bigQueryService, String project, String dataset, String table) {
        return existsTable(bigQueryService, project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table));
    }

    static public boolean existsTable(BigQuery bigQueryService, String dataset, String table) {
        return existsTable(bigQueryService, null, dataset, table);
    }

    static public void createTable(BigQuery bigQueryService, TableId tableId, TableDefinition tableDefinition) {
        createDataset(bigQueryService, tableId.getProject(), tableId.getDataset());

        if (!existsTable(bigQueryService, tableId))
            bigQueryService.create(TableInfo.of(tableId, tableDefinition));

    }

    static public void createTable(BigQuery bigQueryService, String project, String dataset, String table, TableDefinition tableDefinition) {
        createTable(bigQueryService, project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table), tableDefinition);
    }

    static public void createTable(BigQuery bigQueryService, String dataset, String table, TableDefinition tableDefinition) {
        createTable(bigQueryService, null, dataset, table, tableDefinition);
    }

    static public void createTable(BigQuery bigQueryService, TableInfo tableInfo) {
        createTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable(), tableInfo.getDefinition());
    }

    static public void dropTable(BigQuery bigQueryService, String project, String dataset, String table) {
        bigQueryService.delete(project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table));
    }

    static public void dropTable(BigQuery bigQueryService, String dataset, String table) {
        dropTable(bigQueryService, null, table);
    }

    static public void dropTable(BigQuery bigQueryService, TableId tableId) {
        dropTable(bigQueryService, tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

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
