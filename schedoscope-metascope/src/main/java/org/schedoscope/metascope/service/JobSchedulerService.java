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
package org.schedoscope.metascope.service;

import javax.sql.DataSource;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.jobs.LastDataJob;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class JobSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerService.class);

  @Autowired
  private RepositoryDAO repo;
  @Autowired
  private DataSource dataSource;
  @Autowired
  private MetascopeConfig config;
  @Autowired
  @Qualifier("taskExecutor")
  private TaskExecutor executor;

  public void updateLastDataForTable(TableEntity tableEntity) {
    if (tableEntity.getTimestampField() != null) {
      LOG.info("Scheduled LastDataJob for table '{}'", tableEntity.getFqdn());
      executor.execute(new LastDataJob(tableEntity, repo, dataSource, config, System.currentTimeMillis()));
    }
  }

}
