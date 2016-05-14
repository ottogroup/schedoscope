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

import javax.sql.DataSource;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeUtil;

public abstract class Task {

	protected RepositoryDAO repo;
	protected DataSource dataSource;
	protected SolrFacade solr;
	protected MetascopeConfig config;
	protected SchedoscopeUtil schedoscopeUtil;

	public Task(RepositoryDAO repo, DataSource dataSource, SolrFacade solr,
			MetascopeConfig config, SchedoscopeUtil schedoscopeUtil) {
		this.repo = repo;
		this.dataSource = dataSource;
		this.solr = solr;
		this.config = config;
		this.schedoscopeUtil = schedoscopeUtil;
	}

	public void preExecute() {
	}

	public boolean execute(long start) {
		preExecute();
		return run(start);
	}

	public abstract boolean run(long start);

}
