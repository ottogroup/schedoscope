/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.index;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.schedoscope.metascope.index.model.SolrQueryResult;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.service.FieldEntityService;
import org.schedoscope.metascope.service.TableEntityService;
import org.schedoscope.metascope.service.ViewEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class SolrFacade {

	private static final String SOLR_HTTP_PREFIX = "http";
	private static final String METASCOPE_CORE = "metascope";

	@Autowired
	private TableEntityService tableEntityService;
	@Autowired
	private ViewEntityService viewEntityService;
	@Autowired
	private FieldEntityService fieldEntityService;

	private String solrUrl;
	private SolrClient solrClient;
	private SolrUpdateHandler solrUpdateHandler;
	private SolrQueryExecutor solrQueryExecutor;

	public SolrFacade() {
	}

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
				initSolrFacade(new EmbeddedSolrServer(coreContainer,
						METASCOPE_CORE));
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
	 * Refer to
	 * {@link SolrUpdateHandler#updateTableEntity(TableEntity, boolean)}
	 */
	public void updateTableEntity(TableEntity tableEntity, boolean commit) {
		solrUpdateHandler.updateTableEntity(tableEntity, commit);
	}

	/**
	 * Refer to
	 * {@link SolrUpdateHandler#updateTableEntity(TableEntity, boolean)}. The
	 * method is executed asynchronous.
	 * 
	 * @return future to wait for completion
	 */
	@Async
	public Future<Void> updateTableEntityAsync(TableEntity tableEntity,
			boolean commit) {
		return solrUpdateHandler.updateTableEntityAsync(tableEntity, commit);
	}

	/**
	 * Refer to
	 * {@link SolrUpdateHandler#updateTablePartial(TableEntity, boolean)} .
	 */
	public void updateTablePartial(TableEntity tableEntity, boolean commit) {
		solrUpdateHandler.updateTablePartial(tableEntity, commit);
	}

	/**
	 * Refer to
	 * {@link SolrUpdateHandler#updateTableStatusInformation(TableEntity, Long, boolean)}
	 */
	public void updateTableStatusInformation(TableEntity tableEntity,
			Long lastTransformation, boolean commit) {
		solrUpdateHandler.updateTableStatusInformation(tableEntity,
				lastTransformation, commit);
	}

	/**
	 * Refer to {@link SolrUpdateHandler#updateViewEntity(ViewEntity, boolean)}
	 */
	public void updateViewEntity(ViewEntity viewEntity, boolean commit) {
		solrUpdateHandler.updateViewEntity(viewEntity, commit);
	}

	/**
	 * Refer to {@link SolrUpdateHandler#updateViewEntity(ViewEntity, boolean)}.
	 * The method is executed asynchronous.
	 * 
	 * @return future to wait for completion
	 */
	@Async
	public Future<Void> updateViewEntityAsync(ViewEntity viewEntity,
			boolean commit) {
		return solrUpdateHandler.updateViewEntityAsync(viewEntity, commit);
	}

	/**
	 * Refer to
	 * {@link SolrUpdateHandler#updateViewStatusInformation(ViewEntity, Long, Long, boolean)}
	 */
	public void updateViewStatusInformation(ViewEntity viewEntity,
			Long transformationEnd, Long createdAt, boolean commit) {
		solrUpdateHandler.updateViewStatusInformation(viewEntity,
				transformationEnd, createdAt, commit);
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
		this.solrQueryExecutor = new SolrQueryExecutor(solrClient,
				tableEntityService, viewEntityService, fieldEntityService);
	}

}
