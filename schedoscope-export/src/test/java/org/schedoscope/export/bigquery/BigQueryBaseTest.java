package org.schedoscope.export.bigquery;

import com.google.cloud.bigquery.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;

public abstract class BigQueryBaseTest {

    final private static boolean CALL_BIG_QUERY = false;

    final private static boolean CLEAN_UP_BIG_QUERY = true;

    private static BigQueryUtils execute = new BigQueryUtils();

    private static BigQuery bigQuery;


    public void createTable(TableInfo tableInfo) {

        if (CALL_BIG_QUERY) {

            try {

                execute.dropTable(bigQuery, tableInfo);
                execute.createTable(bigQuery, tableInfo);

            } catch (Throwable t) {
                t.printStackTrace();

                try {
                    Thread.currentThread().sleep(500);
                } catch (InterruptedException e) {
                }

                createTable(tableInfo);
            }

        }
    }

    public void insertIntoTable(String dataset, String table, Schema schema, Map<String, Object>... data) {
        if (CALL_BIG_QUERY) {
            TableId tableId = TableId.of(dataset, table);
            TableInfo tableInfo = TableInfo.of(tableId, StandardTableDefinition.newBuilder().setSchema(schema).build());
            createTable(tableInfo);

            try {
                execute.insertIntoTable(bigQuery, tableId, data);
            } catch (Throwable t) {
                t.printStackTrace();
                try {
                    Thread.currentThread().sleep(500);
                } catch (InterruptedException e) {
                }

                insertIntoTable(dataset, table, schema, data);
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

        if (execute.existsDataset(bigQuery, "schedoscope_export_big_query_record_test"))
            execute.dropDataset(bigQuery, "schedoscope_export_big_query_record_test");

        execute.createDataset(bigQuery, "schedoscope_export_big_query_schema_test");
        execute.createDataset(bigQuery, "schedoscope_export_big_query_record_test");
    }

    @AfterClass
    public static void dropBigQueryDataSets() {
        if (!CALL_BIG_QUERY || !CLEAN_UP_BIG_QUERY)
            return;

        if (execute.existsDataset(bigQuery, "schedoscope_export_big_query_schema_test"))
            execute.dropDataset(bigQuery, "schedoscope_export_big_query_schema_test");

        if (execute.existsDataset(bigQuery, "schedoscope_export_big_query_record_test"))
            execute.dropDataset(bigQuery, "schedoscope_export_big_query_record_test");
    }


}
