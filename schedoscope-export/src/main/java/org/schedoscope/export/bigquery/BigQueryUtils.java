package org.schedoscope.export.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public class BigQueryUtils {

    public BigQuery bigQueryService() {
        return BigQueryOptions.getDefaultInstance().getService();
    }


    public BigQuery bigQueryService(String gcpKey) throws IOException {
        if (gcpKey == null)
            return bigQueryService();

        GoogleCredentials credentials = GoogleCredentials
                .fromStream(
                        new ByteArrayInputStream(Charset.forName("UTF-8").encode(gcpKey).array())
                );

        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    public boolean existsDataset(BigQuery bigQueryService, String project, String dataset) {
        return bigQueryService.getDataset(project == null ? DatasetId.of(dataset) : DatasetId.of(project, dataset)) != null;
    }

    public boolean existsDataset(BigQuery bigQueryService, String dataset) {
        return existsDataset(bigQueryService, null, dataset);
    }

    public boolean existsDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        return existsDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    public void createDataset(BigQuery bigQueryService, String project, String dataset) {
        if (!existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.create((project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build());
        }
    }

    public void createDataset(BigQuery bigQueryService, String dataset) {
        createDataset(bigQueryService, null, dataset);
    }

    public void createDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        createDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    public void dropDataset(BigQuery bigQueryService, String project, String dataset) {
        if (existsDataset(bigQueryService, project, dataset)) {
            bigQueryService.delete(
                    (project == null ? DatasetInfo.newBuilder(dataset) : DatasetInfo.newBuilder(project, dataset)).build().getDatasetId(),
                    BigQuery.DatasetDeleteOption.deleteContents()
            );
        }
    }

    public void dropDataset(BigQuery bigQueryService, String dataset) {
        dropDataset(bigQueryService, null, dataset);
    }

    public void dropDataset(BigQuery bigQueryService, DatasetInfo datasetInfo) {
        dropDataset(bigQueryService, datasetInfo.getDatasetId().getProject(), datasetInfo.getDatasetId().getDataset());
    }

    public boolean existsTable(BigQuery bigQueryService, String project, String dataset, String table) {
        return bigQueryService.getTable(project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table)) != null;
    }

    public boolean existsTable(BigQuery bigQueryService, String dataset, String table) {
        return existsTable(bigQueryService, null, table);
    }

    public boolean existsTable(BigQuery bigQueryService, TableInfo tableInfo) {
        return existsTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable());
    }

    public void createTable(BigQuery bigQueryService, String project, String dataset, String table, TableDefinition tableDefinition) {
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

    public void createTable(BigQuery bigQueryService, String dataset, String table, TableDefinition tableDefinition) {
        createTable(bigQueryService, null, dataset, table, tableDefinition);
    }

    public void createTable(BigQuery bigQueryService, TableInfo tableInfo) {
        createTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable(), tableInfo.getDefinition());
    }

    public void dropTable(BigQuery bigQueryService, String project, String dataset, String table) {
        bigQueryService.delete(project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table));
    }

    public void dropTable(BigQuery bigQueryService, String dataset, String table) {
        dropTable(bigQueryService, null, table);
    }

    public void dropTable(BigQuery bigQueryService, TableInfo tableInfo) {
        dropTable(bigQueryService, tableInfo.getTableId().getProject(), tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable());
    }
}
