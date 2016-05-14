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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetascopeTask implements Runnable {

	private static final Logger LOG = LoggerFactory
			.getLogger(MetascopeTask.class);

	private List<Task> tasks;
	private ScheduledThreadPoolExecutor executor;

	public MetascopeTask(RepositoryDAO repo, DataSource dataSource,
			SolrFacade solr, MetascopeConfig config,
			SchedoscopeUtil schedoscopUtil) {
		this.tasks = new ArrayList<Task>();
		this.tasks.add(new SchedoscopeSyncTask(repo, dataSource, solr, config,
				schedoscopUtil));
		this.tasks.add(new SetMetadataTask(repo, dataSource, solr, config,
				schedoscopUtil));
		this.tasks.add(new MetastoreSyncTask(repo, dataSource, solr, config,
				schedoscopUtil));
		this.tasks.add(new LastDataTask(repo, dataSource, solr, config,
				schedoscopUtil));
	}

	@Override
	public void run() {
		boolean allTasksFinishedSuccessfully = true;
		long start = System.currentTimeMillis();
		for (Task task : tasks) {
			boolean success = task.execute(start);
			if (!success) {
				allTasksFinishedSuccessfully = false;
				break;
			}
		}

		if (!allTasksFinishedSuccessfully) {
			LOG.warn("Subtask failed, rescheduling MetaschopeTask in 30 seconds");
			executor.schedule(this, 30, TimeUnit.SECONDS);
			return;
		}

		LOG.warn("MetaschopeTask finished successfully, rescheduling in 24 hours");
		executor.schedule(this, 24, TimeUnit.HOURS);
	}

	public void setExecutor(ScheduledThreadPoolExecutor executor) {
		this.executor = executor;
	}

}
