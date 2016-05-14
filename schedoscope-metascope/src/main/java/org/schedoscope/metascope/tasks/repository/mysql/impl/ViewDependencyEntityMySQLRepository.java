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
package org.schedoscope.metascope.tasks.repository.mysql.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ViewDependencyEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewDependencyEntityMySQLRepository implements
		MySQLRepository<ViewDependencyEntity> {

	private static final Logger LOG = LoggerFactory
			.getLogger(ViewDependencyEntityMySQLRepository.class);

	@Override
	public void insertOrUpdate(Connection connection,
			ViewDependencyEntity viewDependencyEntity) {
		String insertDependencySql = "insert into view_dependency_entity ("
				+ JDBCUtil
						.getDatabaseColumnsForClass(ViewDependencyEntity.class)
				+ ") values ("
				+ JDBCUtil.getValuesCountForClass(ViewDependencyEntity.class)
				+ ") " + "on duplicate key update "
				+ MySQLUtil.getOnDuplicateKeyString(ViewDependencyEntity.class);
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(insertDependencySql);
			stmt.setString(1, viewDependencyEntity.getUrlPath());
			stmt.setString(2, viewDependencyEntity.getDependencyUrlPath());
			stmt.setString(3, viewDependencyEntity.getDependencyFqdn());
			stmt.setString(4, viewDependencyEntity.getInternalViewId());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not save view dependency", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public void insertOrUpdate(Connection connection,
			List<ViewDependencyEntity> dependencies) {
		String insertDependencySql = "insert into view_dependency_entity ("
				+ JDBCUtil
						.getDatabaseColumnsForClass(ViewDependencyEntity.class)
				+ ") values ("
				+ JDBCUtil.getValuesCountForClass(ViewDependencyEntity.class)
				+ ") " + "on duplicate key update "
				+ MySQLUtil.getOnDuplicateKeyString(ViewDependencyEntity.class);
		PreparedStatement stmt = null;
		try {
			int batch = 0;
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement(insertDependencySql);
			for (ViewDependencyEntity viewDependencyEntity : dependencies) {
				stmt.setString(1, viewDependencyEntity.getUrlPath());
				stmt.setString(2, viewDependencyEntity.getDependencyUrlPath());
				stmt.setString(3, viewDependencyEntity.getDependencyFqdn());
				stmt.setString(4, viewDependencyEntity.getInternalViewId());
				stmt.addBatch();
				batch++;
				if (batch % 1024 == 0) {
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			connection.commit();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			LOG.error("Could not save view dependency", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

}
