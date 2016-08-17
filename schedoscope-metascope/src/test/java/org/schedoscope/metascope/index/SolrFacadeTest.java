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
package org.schedoscope.metascope.index;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.index.model.SolrQueryResult;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.util.URLUtil;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SolrFacadeTest extends SpringTest {

    private static final String HIVE_TRANSFORMATION_TYPE = "hive";
    private static final String PIG_TRANSFORMATION_TYPE = "pig";
    private static final String MATERIALIZED_STATUS = "materialized";
    private static final String NODATA_STATUS = "nodata";

    private static boolean init = false;

    @Before
    public void setupLocal() {
        if (init == false) {
            solr.clearSolrData();
            init = true;
        }
    }

    @Test
    public void solrFacade_01_createTableEntity() {
        SolrInputDocument tableDocument = solr.getById(getTestTable().getFqdn());

        assertEquals(tableDocument, null);

        solr.updateTableEntity(getTestTable(), true);

        tableDocument = solr.getById(getTestTable().getFqdn());

        assertTrue(tableDocument != null);
    }

    @Test
    public void solrFacade_02_updateTableEntity() {
        SolrInputDocument tableDocument = solr.getById(getTestTable().getFqdn());

        assertTrue(tableDocument != null);

        String transformation = (String) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATION);

        assertEquals(transformation, HIVE_TRANSFORMATION_TYPE);

        TableEntity tableEntity = getTestTable();
        tableEntity.setTransformationType(PIG_TRANSFORMATION_TYPE);

        solr.updateTableEntity(tableEntity, true);

        tableDocument = solr.getById(tableEntity.getFqdn());

        String newTransformation = (String) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATION);

        assertEquals(newTransformation, PIG_TRANSFORMATION_TYPE);
    }

    @Test
    public void solrFacade_03_updateTableEntityAsync() {
        SolrInputDocument tableDocument = solr.getById(getTestTable().getFqdn());

        assertTrue(tableDocument != null);

        String transformation = (String) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATION);

        assertEquals(transformation, PIG_TRANSFORMATION_TYPE);

        TableEntity tableEntity = getTestTable();
        tableEntity.setTransformationType(HIVE_TRANSFORMATION_TYPE);

        Future<Void> tableEntityAsync = solr.updateTableEntityAsync(tableEntity, true);

        // wait for completion
        try {
            tableEntityAsync.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tableDocument = solr.getById(tableEntity.getFqdn());

        String newTransformation = (String) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATION);

        assertEquals(newTransformation, HIVE_TRANSFORMATION_TYPE);
    }

    @Test
    public void solrFacade_04_updateTablePartial() {
        SolrInputDocument tableDocument = solr.getById(getTestTable().getFqdn());

        assertTrue(tableDocument != null);

        String transformation = (String) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATION);

        assertEquals(transformation, HIVE_TRANSFORMATION_TYPE);

        TableEntity tableEntity = getTestTable();
        tableEntity.setTransformationType(PIG_TRANSFORMATION_TYPE);

        solr.updateTablePartial(tableEntity, true);

        tableDocument = solr.getById(tableEntity.getFqdn());

        String newTransformation = (String) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATION);

        assertEquals(newTransformation, PIG_TRANSFORMATION_TYPE);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void solrFacade_05_updateTableStatus() {
        SolrInputDocument tableDocument = solr.getById(getTestTable().getFqdn());

        assertTrue(tableDocument != null);

        String status = (String) tableDocument.getFieldValue(SolrUpdateHandler.STATUS);
        Object transformationEnd = tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATIONTIMESTAMP);

        assertEquals(status, MATERIALIZED_STATUS);
        assertEquals(transformationEnd, null);

        TableEntity tableEntity = getTestTable();
        tableEntity.setStatus(NODATA_STATUS);
        long ts = System.currentTimeMillis();

        solr.updateTableStatusInformation(tableEntity, ts, true);

        tableDocument = solr.getById(getTestTable().getFqdn());

        status = (String) tableDocument.getFieldValue(SolrUpdateHandler.STATUS);
        long newTransformationEnd = (long) tableDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATIONTIMESTAMP);

        assertEquals(status, NODATA_STATUS);
        assertEquals(ts / 1000, newTransformationEnd);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void solrFacade_06_createViewEntity() {
        SolrInputDocument viewDocument = solr.getById(getTestView().getUrlPath());

        assertEquals(viewDocument, null);

        solr.updateViewEntity(getTestView(), true);

        viewDocument = solr.getById(getTestView().getUrlPath());

        assertTrue(viewDocument != null);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void solrFacade_07_updateViewEntity() {
        SolrInputDocument viewDocument = solr.getById(getTestView().getUrlPath());

        assertTrue(viewDocument != null);

        String status = (String) viewDocument.getFieldValue(SolrUpdateHandler.STATUS);

        assertEquals(status, MATERIALIZED_STATUS);

        ViewEntity viewEntity = getTestView();
        viewEntity.setStatus(NODATA_STATUS);

        solr.updateViewEntity(viewEntity, true);

        viewDocument = solr.getById(viewEntity.getUrlPath());

        String newStatus = (String) viewDocument.getFieldValue(SolrUpdateHandler.STATUS);

        assertEquals(newStatus, NODATA_STATUS);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void solrFacade_08_updateViewEntityAsync() {
        SolrInputDocument viewDocument = solr.getById(getTestView().getUrlPath());

        assertTrue(viewDocument != null);

        String status = (String) viewDocument.getFieldValue(SolrUpdateHandler.STATUS);

        assertEquals(status, NODATA_STATUS);

        ViewEntity viewEntity = getTestView();
        viewEntity.setStatus(MATERIALIZED_STATUS);

        Future<Void> viewEntityAsync = solr.updateViewEntityAsync(viewEntity, true);

        // wait for completion
        try {
            viewEntityAsync.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        viewDocument = solr.getById(viewEntity.getUrlPath());

        String newStatus = (String) viewDocument.getFieldValue(SolrUpdateHandler.STATUS);

        assertEquals(newStatus, MATERIALIZED_STATUS);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void solrFacade_09_updateViewStatus() {
        SolrInputDocument viewDocument = solr.getById(getTestView().getUrlPath());

        assertTrue(viewDocument != null);

        String status = (String) viewDocument.getFieldValue(SolrUpdateHandler.STATUS);
        long transformationEnd = (long) viewDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATIONTIMESTAMP);
        long createdAt = (long) viewDocument.getFieldValue(SolrUpdateHandler.CREATED_AT);

        assertEquals(status, MATERIALIZED_STATUS);
        assertEquals(transformationEnd, 0);
        assertEquals(createdAt, 0);

        long ts = System.currentTimeMillis();
        ViewEntity viewEntity = getTestView();
        viewEntity.setStatus(NODATA_STATUS);

        solr.updateViewStatusInformation(viewEntity, ts, ts, true);

        viewDocument = solr.getById(getTestView().getUrlPath());

        status = (String) viewDocument.getFieldValue(SolrUpdateHandler.STATUS);
        long newTransformationEnd = (long) viewDocument.getFieldValue(SolrUpdateHandler.TRANSFORMATIONTIMESTAMP);
        long newCreatedAt = (long) viewDocument.getFieldValue(SolrUpdateHandler.CREATED_AT);

        assertEquals(status, NODATA_STATUS);
        assertEquals(ts / 1000, newTransformationEnd);
        assertEquals(ts / 1000, newCreatedAt);
    }

    @Test
    public void solrFacade_10_testQueryPlain() {
        SolrQueryResult queryResult = solr.query(new HashMap<String, String>());

        assertEquals(queryResult.getPage(), URLUtil.PAGINATION_PAGE_DEFAULT);
        assertEquals(queryResult.getElements(), URLUtil.PAGINATION_ELEMENTS_DEFAULT);
        assertEquals(queryResult.getResultType(), SolrQueryExecutor.TYPE_TABLE);
        assertEquals(queryResult.getStart(), 1);
        assertEquals(queryResult.getTotalResults(), 1);
        for (Entry<String, List<String>> e : queryResult.getActiveFilterValues().entrySet()) {
            assertEquals(e.getValue().size(), 0);
        }
    }

    @Test
    public void solrFacade_11_testQueryPartitions() {
        HashMap<String, String> parameter = new HashMap<String, String>();

        parameter.put("type", SolrQueryExecutor.TYPE_PARTITION);

        SolrQueryResult queryResult = solr.query(parameter);

        assertEquals(queryResult.getPage(), URLUtil.PAGINATION_PAGE_DEFAULT);
        assertEquals(queryResult.getElements(), URLUtil.PAGINATION_ELEMENTS_DEFAULT);
        assertEquals(queryResult.getResultType(), SolrQueryExecutor.TYPE_PARTITION);
        assertEquals(queryResult.getStart(), 1);
        assertEquals(queryResult.getTotalResults(), 1);
        for (Entry<String, List<String>> e : queryResult.getActiveFilterValues().entrySet()) {
            assertEquals(e.getValue().size(), 0);
        }
    }

    @Test
    public void solrFacade_12_testQueryWithSearchQuery() {
        HashMap<String, String> parameter = new HashMap<String, String>();

        parameter.put(URLUtil.SEARCH_QUERY_PARAM, PIG_TRANSFORMATION_TYPE);

        SolrQueryResult queryResult = solr.query(parameter);

        assertEquals(queryResult.getPage(), URLUtil.PAGINATION_PAGE_DEFAULT);
        assertEquals(queryResult.getElements(), URLUtil.PAGINATION_ELEMENTS_DEFAULT);
        assertEquals(queryResult.getResultType(), SolrQueryExecutor.TYPE_TABLE);
        assertEquals(queryResult.getStart(), 1);
        assertEquals(queryResult.getTotalResults(), 1);
        for (Entry<String, List<String>> e : queryResult.getActiveFilterValues().entrySet()) {
            assertEquals(e.getValue().size(), 0);
        }
    }

    @Test
    public void solrFacade_13_testQueryWithDatabaseFilter() {
        HashMap<String, String> parameter = new HashMap<String, String>();

        parameter.put(SolrQueryExecutor.DATABASE_NAME, getTestTable().getDatabaseName());

        SolrQueryResult queryResult = solr.query(parameter);

        assertEquals(queryResult.getPage(), URLUtil.PAGINATION_PAGE_DEFAULT);
        assertEquals(queryResult.getElements(), URLUtil.PAGINATION_ELEMENTS_DEFAULT);
        assertEquals(queryResult.getResultType(), SolrQueryExecutor.TYPE_TABLE);
        assertEquals(queryResult.getStart(), 1);
        assertEquals(queryResult.getTotalResults(), 1);
        for (Entry<String, List<String>> e : queryResult.getActiveFilterValues().entrySet()) {
            if (e.getKey().equals(SolrQueryExecutor.DATABASE_NAME)) {
                assertEquals(e.getValue().size(), 1);
            } else {
                assertEquals(e.getValue().size(), 0);
            }
        }
    }

    @Test
    public void solrFacade_15_testQueryWithSearchQueryAndDatabaseFilter() {
        HashMap<String, String> parameter = new HashMap<String, String>();

        parameter.put(URLUtil.SEARCH_QUERY_PARAM, PIG_TRANSFORMATION_TYPE);
        parameter.put(SolrQueryExecutor.DATABASE_NAME, getTestTable().getDatabaseName());

        SolrQueryResult queryResult = solr.query(parameter);

        assertEquals(queryResult.getPage(), URLUtil.PAGINATION_PAGE_DEFAULT);
        assertEquals(queryResult.getElements(), URLUtil.PAGINATION_ELEMENTS_DEFAULT);
        assertEquals(queryResult.getResultType(), SolrQueryExecutor.TYPE_TABLE);
        assertEquals(queryResult.getStart(), 1);
        assertEquals(queryResult.getTotalResults(), 1);
        for (Entry<String, List<String>> e : queryResult.getActiveFilterValues().entrySet()) {
            if (e.getKey().equals(SolrQueryExecutor.DATABASE_NAME)) {
                assertEquals(e.getValue().size(), 1);
            } else {
                assertEquals(e.getValue().size(), 0);
            }
        }
    }

    @Test
    public void solrFacade_16_testQueryWithNoResults() {
        HashMap<String, String> parameter = new HashMap<String, String>();

        parameter.put(URLUtil.SEARCH_QUERY_PARAM, "fooBar");

        SolrQueryResult queryResult = solr.query(parameter);

        assertEquals(queryResult.getPage(), URLUtil.PAGINATION_PAGE_DEFAULT);
        assertEquals(queryResult.getElements(), URLUtil.PAGINATION_ELEMENTS_DEFAULT);
        assertEquals(queryResult.getResultType(), SolrQueryExecutor.TYPE_TABLE);
        assertEquals(queryResult.getStart(), 1);
        assertEquals(queryResult.getTotalResults(), 0);
        for (Entry<String, List<String>> e : queryResult.getActiveFilterValues().entrySet()) {
            assertEquals(e.getValue().size(), 0);
        }
    }

}
