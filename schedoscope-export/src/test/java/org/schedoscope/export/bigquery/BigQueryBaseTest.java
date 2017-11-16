package org.schedoscope.export.bigquery;

import com.google.cloud.bigquery.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BigQueryBaseTest {

    private static boolean CALL_BIG_QUERY = false;

    private static boolean CLEAN_UP_BIG_QUERY = true;

    private static BigQuery bigQuery;

    public void createTable(TableInfo tableInfo) {

        if (CALL_BIG_QUERY) {

            if (bigQuery.getTable(tableInfo.getTableId()) != null)
                bigQuery.delete(tableInfo.getTableId());

            bigQuery.create(tableInfo);

            try {
                Thread.currentThread().sleep(500);
            } catch (InterruptedException e) {
            }

        }
    }

    @BeforeClass
    public static void createBigQueryDataSet() {
        if (!CALL_BIG_QUERY)
            return;

        bigQuery = BigQueryOptions.getDefaultInstance().getService();

        DatasetId datasetId = DatasetId.of("schedoscope_export_big_query_schema_test");
        bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());

        DatasetInfo datasetInfo = DatasetInfo.newBuilder("schedoscope_export_big_query_schema_test").build();
        bigQuery.create(datasetInfo);
    }

    @AfterClass
    public static void dropBigQueryDataSets() {
        if (!CALL_BIG_QUERY || !CLEAN_UP_BIG_QUERY)
            return;

        DatasetId datasetId = DatasetId.of("schedoscope_export_big_query_schema_test");
        bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    }


}
