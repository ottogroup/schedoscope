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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.schedoscope.metascope.index.model.SolrQueryResult;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeView;
import org.schedoscope.metascope.service.MetascopeFieldService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.schedoscope.metascope.service.MetascopeViewService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Component
public class SolrFacade {

  private static final String SOLR_HTTP_PREFIX = "http";
  private static final String METASCOPE_CORE = "metascope";

  @Autowired
  private MetascopeTableService metascopeTableService;
  @Autowired
  private MetascopeViewService metascopeViewService;
  @Autowired
  private MetascopeFieldService metascopeParameterService;

  private String solrUrl;
  private SolrClient solrClient;
  private SolrUpdateHandler solrUpdateHandler;
  private SolrQueryExecutor solrQueryExecutor;

  public SolrFacade(String solrUrl) {
    this.solrUrl = solrUrl;
  }

  @PostConstruct
  public void init() {
    if (solrClient == null) {
      String url = solrUrl;
      if (url.startsWith(SOLR_HTTP_PREFIX)) {
        initSolrFacade(new HttpSolrClient(url));
      } else {
        CoreContainer coreContainer = new CoreContainer(url);
        coreContainer.load();
        initSolrFacade(new EmbeddedSolrServer(coreContainer, METASCOPE_CORE));
      }
    }
  }

  /**
   * Refer to {@link SolrUpdateHandler#getDocument(String)}
   *
   * @return
   */
  public SolrInputDocument getById(String id) {
    return solrUpdateHandler.getDocument(id);
  }

  /**
   * Refer to {@link SolrUpdateHandler#clearSolrData()}
   *
   * @return
   */
  public void clearSolrData() {
    solrUpdateHandler.clearSolrData();
  }

  /**
   * Refer to {@link SolrUpdateHandler#updateTableEntity(MetascopeTable, boolean)}
   */
  public void updateTableEntity(MetascopeTable tableEntity, boolean commit) {
    solrUpdateHandler.updateTableEntity(tableEntity, commit);
  }

  /**
   * Refer to {@link SolrUpdateHandler#updateTableEntity(MetascopeTable, boolean)}.
   * The method is executed asynchronous.
   *
   * @return future to wait for completion
   */
  @Async
  public Future<Void> updateTableEntityAsync(MetascopeTable table, boolean commit) {
    return solrUpdateHandler.updateTableEntityAsync(table, commit);
  }

  /**
   * Refer to {@link SolrUpdateHandler#updateTablePartial(MetascopeTable, boolean)}
   * .
   */
  public void updateTablePartial(MetascopeTable table, boolean commit) {
    solrUpdateHandler.updateTablePartial(table, commit);
  }

  /**
   * Refer to {@link SolrUpdateHandler#updateViewEntity(MetascopeView, boolean)}
   */
  public void updateViewEntity(MetascopeView view, boolean commit) {
    solrUpdateHandler.updateViewEntity(view, commit);
  }

  /**
   * Refer to {@link SolrUpdateHandler#updateViewEntity(MetascopeView, boolean)}.
   * The method is executed asynchronous.
   *
   * @return future to wait for completion
   */
  @Async
  public Future<Void> updateViewEntityAsync(MetascopeView view, boolean commit) {
    return solrUpdateHandler.updateViewEntityAsync(view, commit);
  }

  /**
   * Refer to {@link SolrUpdateHandler#commit()}
   */
  public void commit() {
    solrUpdateHandler.commit();
  }

  /**
   * Refer to {@link SolrQueryExecutor#suggest(String)}
   */
  public List<String> suggest(String userInput) {
    return solrQueryExecutor.suggest(userInput);
  }

  /**
   * Refer to {@link SolrQueryExecutor#query(Map)}
   */
  public SolrQueryResult query(Map<String, String> params) {
    return solrQueryExecutor.query(params);
  }

  public void initSolrFacade(SolrClient solrClient) {
    this.solrClient = solrClient;
    this.solrUpdateHandler = new SolrUpdateHandler(solrClient);
    this.solrQueryExecutor = new SolrQueryExecutor(solrClient, metascopeTableService,
            metascopeViewService, metascopeParameterService);
  }

}
