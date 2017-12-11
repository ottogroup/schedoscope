package org.schedoscope.export.bigquery;

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

    public static BigQuery bigQueryService() {
        return BigQueryOptions.getDefaultInstance().getService();
    }


    public static BigQuery bigQueryService(String gcpKey) throws IOException {
        if (gcpKey == null)
            return bigQueryService();

        GoogleCredentials credentials = GoogleCredentials
                .fromStream(
                        new ByteArrayInputStream(Charset.forName("UTF-8").encode(gcpKey).array())
                );

        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    public static <T> T retry(int numberOfRetries, Supplier<T> action) {
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

    public static void retry(int numberOfRetries, Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            if (numberOfRetries > 0) {

                try {
                    Thread.currentThread().sleep(rnd.nextInt(2000));
                } catch (InterruptedException e) {
                }

                retry(numberOfRetries - 1, action);
            } else
                throw t;
        }
    }

    public static boolean existsDataset(BigQuery bigQueryService, String project, String dataset) {
        return bigQueryService.getDataset(project == null ? DatasetId.of(dataset) : DatasetId.of(project, dataset)) != null;
    }

    public static boolean existsDataset(BigQuery bigQueryService, String dataset) {
        return existsDataset(bigQueryService, null, dataset);
    }

    public static boolean existsDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        return existsDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    public static void createDataset(BigQuery bigQueryService, String project, String dataset) {
        if (!existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.create((project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build());
        }
    }

    public static void createDataset(BigQuery bigQueryService, String dataset) {
        createDataset(bigQueryService, null, dataset);
    }

    public static void createDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        createDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    public static void dropDataset(BigQuery bigQueryService, String project, String dataset) {
        if (existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.delete(
                    (project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build().getDatasetId(),
                    BigQuery.DatasetDeleteOption.deleteContents()
            );
        }
    }

    public static void dropDataset(BigQuery bigQueryService, String dataset) {
        dropDataset(bigQueryService, null, dataset);
    }

    public static void dropDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        dropDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    public static boolean existsTable(BigQuery bigQueryService, String project, String dataset, String table) {
        return bigQueryService.getTable(project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table)) != null;
    }

    public static boolean existsTable(BigQuery bigQueryService, String dataset, String table) {
        return existsTable(bigQueryService, null, table);
    }

    public static boolean existsTable(BigQuery bigQueryService, TableInfo tableInfo) {
        return existsTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable());
    }

    public static void createTable(BigQuery bigQueryService, String project, String dataset, String table, TableDefinition tableDefinition) {
        createDataset(bigQueryService, project, dataset);

        if (!existsTable(bigQueryService, project, dataset, table)) {
            bigQueryService.create(
                    TableInfo.of(
                            project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table),
                            tableDefinition
                    )
            );
        }
    }

    public static void createTable(BigQuery bigQueryService, String dataset, String table, TableDefinition tableDefinition) {
        createTable(bigQueryService, null, dataset, table, tableDefinition);
    }

    public static void createTable(BigQuery bigQueryService, TableInfo tableInfo) {
        createTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable(), tableInfo.getDefinition());
    }

    public static void dropTable(BigQuery bigQueryService, String project, String dataset, String table) {
        bigQueryService.delete(project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table));
    }

    public static void dropTable(BigQuery bigQueryService, String dataset, String table) {
        dropTable(bigQueryService, null, table);
    }

    public static void dropTable(BigQuery bigQueryService, TableInfo tableInfo) {
        dropTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable());
    }

    public static void insertIntoTable(BigQuery bigQueryService, TableId table, Map<String, Object>... rowsToInsert) {

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
