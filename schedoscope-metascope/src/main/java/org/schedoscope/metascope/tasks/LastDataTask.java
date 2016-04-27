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
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.sql.DataSource;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.jobs.LastDataJob;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastDataTask extends Task {

  private static final Logger LOG = LoggerFactory.getLogger(LastDataTask.class);

  private ScheduledThreadPoolExecutor executor;

  public LastDataTask(RepositoryDAO repo, DataSource dataSource, SolrFacade solr, MetascopeConfig config,
      SchedoscopeUtil schedoscopeUtil) {
    super(repo, dataSource, solr, config, schedoscopeUtil);
    this.executor = new ScheduledThreadPoolExecutor(4);
  }

  public boolean run(long start) {
    Connection connection;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      LOG.info("[LastDataTask] FAILED: Could not connect to repository", e);
      return false;
    }

    List<TableEntity> tables = repo.getTables(connection);
    for (TableEntity tableEntity : tables) {
      if (tableEntity.getTimestampField() != null
          && (tableEntity.getLastChange() == start || tableEntity.getLastData() == null)) {
        LOG.info("Scheduled LastDataJob for table '{}'", tableEntity.getFqdn());
        executor.execute(new LastDataJob(tableEntity, repo, dataSource, config, start));
      }
    }

    try {
      connection.close();
    } catch (SQLException e) {
      LOG.error("Could not close connection to repository", e);
    }

    return true;
  }

}
