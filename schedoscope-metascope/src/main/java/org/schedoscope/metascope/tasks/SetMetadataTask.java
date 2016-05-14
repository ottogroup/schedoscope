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

import javax.sql.DataSource;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.Metadata;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetMetadataTask extends Task {

	private static final Logger LOG = LoggerFactory
			.getLogger(SetMetadataTask.class);

	public SetMetadataTask(RepositoryDAO repo, DataSource dataSource,
			SolrFacade solr, MetascopeConfig config,
			SchedoscopeUtil schedoscopUtil) {
		super(repo, dataSource, solr, config, schedoscopUtil);
	}

	@Override
	public boolean run(long start) {
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			LOG.info(
					"[SetMetadataTask] FAILED: Could not connect to repository",
					e);
			return false;
		}

		Metadata lastSync = repo.getMetadata(connection, "timestamp");
		repo.insertOrUpdate(connection, new Metadata("timestamp", "" + start));
		if (lastSync == null) {
			LOG.info("Initialilzed repository. You can now log in");
		}

		try {
			connection.close();
		} catch (SQLException e) {
			LOG.error("Could not close connection to repository", e);
		}
		return true;
	}

}
