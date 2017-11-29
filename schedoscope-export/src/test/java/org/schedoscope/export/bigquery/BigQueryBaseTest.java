package org.schedoscope.export.bigquery;

import com.google.cloud.bigquery.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BigQueryBaseTest {

    final private static boolean CALL_BIG_QUERY = false;

    final private static boolean CLEAN_UP_BIG_QUERY = true;

    private static BigQueryUtils execute = new BigQueryUtils();

    private static BigQuery bigQuery;


    public void createTable(TableInfo tableInfo) {

        if (CALL_BIG_QUERY) {

            execute.dropTable(bigQuery, tableInfo);
            execute.createTable(bigQuery, tableInfo);

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

        bigQuery = execute.bigQueryService();

        if (execute.existsDataset(bigQuery, "schedoscope_export_big_query_schema_test"))
            execute.dropDataset(bigQuery, "schedoscope_export_big_query_schema_test");

        execute.createDataset(bigQuery, "schedoscope_export_big_query_schema_test");
    }

    @AfterClass
    public static void dropBigQueryDataSets() {
        if (!CALL_BIG_QUERY || !CLEAN_UP_BIG_QUERY)
            return;

        if (execute.existsDataset(bigQuery, "schedoscope_export_big_query_schema_test"))
            execute.dropDataset(bigQuery, "schedoscope_export_big_query_schema_test");
    }


}
