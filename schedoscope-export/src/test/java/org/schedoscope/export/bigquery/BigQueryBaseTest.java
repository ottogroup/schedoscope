package org.schedoscope.export.bigquery;

import com.google.cloud.bigquery.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.schedoscope.export.utils.BigQueryUtils;

import java.util.Map;

import static org.schedoscope.export.utils.BigQueryUtils.*;

public abstract class BigQueryBaseTest {

    final private static boolean CALL_BIG_QUERY = false;

    final private static boolean CLEAN_UP_BIG_QUERY = true;

    private static BigQuery bigQuery;


    public void createTable(TableInfo tableInfo) {

        if (CALL_BIG_QUERY)
            retry(3, () -> {
                dropTable(bigQuery, tableInfo.getTableId());
                BigQueryUtils.createTable(bigQuery, tableInfo);
            });


    }

    public void insertIntoTable(String dataset, String table, Schema schema, Map<String, Object>... data) {
        if (CALL_BIG_QUERY) {
            TableId tableId = TableId.of(dataset, table);
            TableInfo tableInfo = TableInfo.of(tableId, StandardTableDefinition.newBuilder().setSchema(schema).build());

            createTable(tableInfo);
            retry(3, () -> BigQueryUtils.insertIntoTable(bigQuery, tableId, data));

        }
    }

    @BeforeClass
    public static void createBigQueryDataSet() {
        if (!CALL_BIG_QUERY)
            return;

        bigQuery = bigQueryService();

        if (existsDataset(bigQuery, "schedoscope_export_big_query_schema_test"))
            dropDataset(bigQuery, "schedoscope_export_big_query_schema_test");

        if (existsDataset(bigQuery, "schedoscope_export_big_query_record_test"))
            dropDataset(bigQuery, "schedoscope_export_big_query_record_test");

        if (existsDataset(bigQuery, "schedoscope_export_big_query_output_test"))
            dropDataset(bigQuery, "schedoscope_export_big_query_output_test");

        createDataset(bigQuery, "schedoscope_export_big_query_schema_test");
        createDataset(bigQuery, "schedoscope_export_big_query_record_test");
        createDataset(bigQuery, "schedoscope_export_big_query_output_test");
    }

    @AfterClass
    public static void dropBigQueryDataSets() {
        if (!CALL_BIG_QUERY || !CLEAN_UP_BIG_QUERY)
            return;

        if (existsDataset(bigQuery, "schedoscope_export_big_query_schema_test"))
            dropDataset(bigQuery, "schedoscope_export_big_query_schema_test");

        if (existsDataset(bigQuery, "schedoscope_export_big_query_record_test"))
            dropDataset(bigQuery, "schedoscope_export_big_query_record_test");

        if (existsDataset(bigQuery, "schedoscope_export_big_query_output_test"))
            dropDataset(bigQuery, "schedoscope_export_big_query_output_test");
    }


}
