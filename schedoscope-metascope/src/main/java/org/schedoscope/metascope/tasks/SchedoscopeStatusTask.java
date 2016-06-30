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
package org.schedoscope.metascope.tasks;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.Metadata;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeConnectException;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedoscopeStatusTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SchedoscopeStatusTask.class);

  private RepositoryDAO repo;
  private DataSource dataSource;
  private SolrFacade solr;
  private SchedoscopeUtil schedoscopeUtil;
  private long lastExceptionPrinted;

  public SchedoscopeStatusTask(RepositoryDAO repo, DataSource dataSource, SolrFacade solr,
      SchedoscopeUtil schedoscopeUtil) {
    this.repo = repo;
    this.dataSource = dataSource;
    this.solr = solr;
    this.schedoscopeUtil = schedoscopeUtil;
    this.lastExceptionPrinted = 0;
  }

  @Override
  public void run() {
    Connection connection;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      LOG.error("Could not connect to repository", e);
      return;
    }

    /* repository not initialized yet */
    if (repo.getMetadata(connection, "timestamp") == null) {
      DbUtils.closeQuietly(connection);
      return;
    }

    Map<String, TableEntity> repositoryTables = createTableMap(repo.getTables(connection));
    List<ViewEntity> repoViews = repo.getViews(connection);
    Map<String, ViewEntity> repositoryViews = createViewMap(repoViews);
    Map<String, List<ViewEntity>> repositoryViewGrouppedByFqdn = createViewListMap(repoViews);

    /* get current view information from Schedoscope */
    List<ViewEntity> views;
    try {
      views = schedoscopeUtil.getViews(false);
    } catch (SchedoscopeConnectException e) {
      if (printException()) {
        LOG.warn("Could not retrieve view status from Schedoscope", e);
        this.lastExceptionPrinted = System.currentTimeMillis();
      }
      return;
    }
    if (views == null || views.size() == 0) {
      repo.insertOrUpdate(connection, new Metadata("status", "offline"));
      DbUtils.closeQuietly(connection);
      return;
    }

    Map<String, ViewEntity> currentViews = createViewMap(views);
    Set<TableEntity> tableStatusChanged = new HashSet<TableEntity>();
    Set<ViewEntity> viewStatusChanged = new HashSet<ViewEntity>();
    for (ViewEntity currentView : currentViews.values()) {
      ViewEntity viewEntity = repositoryViews.get(currentView.getUrlPath());
      if (viewEntity == null) {
        continue;
      }

      /* view status changed */
      if (!currentView.getStatus().equals(viewEntity.getStatus())) {
        /*
         * change status of view, get parent table and update its status
         * accordingly
         */
        TableEntity tableEntity = repositoryTables.get(viewEntity.getFqdn());
        viewEntity.setStatus(currentView.getStatus());
        tableEntity.setStatus(schedoscopeUtil.getStatus(repositoryViewGrouppedByFqdn.get(viewEntity.getFqdn())));
        tableStatusChanged.add(tableEntity);
        viewStatusChanged.add(viewEntity);
      }
    }

    if (viewStatusChanged.size() > 0) {
      repo.updateViewStatus(connection, viewStatusChanged);
      for (ViewEntity viewEntity : viewStatusChanged) {
        solr.updateViewStatusInformation(viewEntity, viewEntity.getTransformationEnd(), null, false);
      }
    }

    if (tableStatusChanged.size() > 0) {
      repo.updateTableStatus(connection, tableStatusChanged);
      for (TableEntity tableEntity : tableStatusChanged) {
        solr.updateTableStatusInformation(tableEntity, tableEntity.getLastTransformation(), false);
      }
    }

    solr.commit();
    DbUtils.closeQuietly(connection);
  }

  /**
   * @return
   */
  private boolean printException() {
    if (System.currentTimeMillis() - lastExceptionPrinted > 60000) {
      return true;
    }
    return false;
  }

  private Map<String, TableEntity> createTableMap(List<TableEntity> tables) {
    Map<String, TableEntity> map = new HashMap<String, TableEntity>();
    for (TableEntity tableEntity : tables) {
      map.put(tableEntity.getFqdn(), tableEntity);
    }
    return map;
  }

  private Map<String, ViewEntity> createViewMap(List<ViewEntity> views) {
    Map<String, ViewEntity> map = new HashMap<String, ViewEntity>();
    for (ViewEntity viewEntity : views) {
      map.put(viewEntity.getUrlPath(), viewEntity);
    }
    return map;
  }

  private Map<String, List<ViewEntity>> createViewListMap(List<ViewEntity> views) {
    Map<String, List<ViewEntity>> map = new HashMap<String, List<ViewEntity>>();
    for (ViewEntity viewEntity : views) {
      List<ViewEntity> list = map.get(viewEntity.getFqdn());
      if (list == null) {
        list = new ArrayList<ViewEntity>();
      }
      list.add(viewEntity);
      map.put(viewEntity.getFqdn(), list);
    }
    return map;
  }

}
