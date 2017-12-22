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
package org.schedoscope.export.bigquery;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Storage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.schedoscope.export.utils.BigQueryUtils;

import java.util.Map;

import static org.schedoscope.export.utils.BigQueryUtils.*;
import static org.schedoscope.export.utils.CloudStorageUtils.*;

public abstract class BigQueryBaseTest {

    final protected static boolean CALL_BIG_QUERY = false;

    final protected static boolean CLEAN_UP_BIG_QUERY = true;

    protected static BigQuery bigQuery;
    protected static Storage storage;


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
        storage = storageService();

        if (existsDataset(bigQuery, null, "schedoscope_export_big_query_schema_test"))
            dropDataset(bigQuery, null, "schedoscope_export_big_query_schema_test");

        if (existsDataset(bigQuery, null, "schedoscope_export_big_query_record_test"))
            dropDataset(bigQuery, null, "schedoscope_export_big_query_record_test");

        if (existsDataset(bigQuery, null, "schedoscope_export_big_query_output_test"))
            dropDataset(bigQuery, null, "schedoscope_export_big_query_output_test");

        if (existsBucket(storage, "schedoscope_export_big_query_output_test"))
            deleteBucket(storage, "schedoscope_export_big_query_output_test");

        createDataset(bigQuery, null, "schedoscope_export_big_query_schema_test", "EU");
        createDataset(bigQuery, null, "schedoscope_export_big_query_record_test", "EU");
        createDataset(bigQuery, null, "schedoscope_export_big_query_output_test", "EU");

        createBucket(storage, "schedoscope_export_big_query_output_test", "europe-west3");

    }

    @AfterClass
    public static void dropBigQueryDataSets() {
        if (!CALL_BIG_QUERY || !CLEAN_UP_BIG_QUERY)
            return;

        if (existsDataset(bigQuery, null, "schedoscope_export_big_query_schema_test"))
            dropDataset(bigQuery, null, "schedoscope_export_big_query_schema_test");

        if (existsDataset(bigQuery, null, "schedoscope_export_big_query_record_test"))
            dropDataset(bigQuery, null, "schedoscope_export_big_query_record_test");

        if (existsDataset(bigQuery, null, "schedoscope_export_big_query_output_test"))
            dropDataset(bigQuery, null, "schedoscope_export_big_query_output_test");

        if (existsBucket(storage, "schedoscope_export_big_query_output_test"))
            deleteBucket(storage, "schedoscope_export_big_query_output_test");
    }


}
