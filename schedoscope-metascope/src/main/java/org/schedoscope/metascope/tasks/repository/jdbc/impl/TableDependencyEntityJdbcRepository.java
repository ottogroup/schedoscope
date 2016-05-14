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
package org.schedoscope.metascope.tasks.repository.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.key.TableDependencyEntityKey;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableDependencyEntityJdbcRepository implements
		JdbcRepository<TableDependencyEntity, TableDependencyEntityKey> {

	private static final Logger LOG = LoggerFactory
			.getLogger(TableDependencyEntityJdbcRepository.class);

	@Override
	public TableDependencyEntity get(Connection connection,
			TableDependencyEntityKey key) {
		TableDependencyEntity dependencyEntity = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection
					.prepareStatement("select "
							+ JDBCUtil
									.getDatabaseColumnsForClass(TableDependencyEntity.class)
							+ " from table_dependency_entity where fqdn = ? and dependency_fqdn = ?");
			stmt.setString(1, key.getFqdn());
			stmt.setString(2, key.getDependencyFqdn());
			rs = stmt.executeQuery();
			if (rs.next()) {
				dependencyEntity = new TableDependencyEntity();
				dependencyEntity.setFqdn(rs
						.getString(TableDependencyEntity.FQDN));
				dependencyEntity.setDependencyFqdn(rs
						.getString(TableDependencyEntity.DEPENDENCY_FQDN));
			}
		} catch (SQLException e) {
			LOG.error("Could not query table dependencies", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return dependencyEntity;
	}

	@Override
	public List<TableDependencyEntity> get(Connection connection) {
		List<TableDependencyEntity> list = new ArrayList<TableDependencyEntity>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection
					.prepareStatement("select "
							+ JDBCUtil
									.getDatabaseColumnsForClass(TableDependencyEntity.class)
							+ " from table_dependency_entity");
			rs = stmt.executeQuery();
			while (rs.next()) {
				TableDependencyEntity dependencyEntity = new TableDependencyEntity();
				dependencyEntity.setFqdn(rs
						.getString(TableDependencyEntity.FQDN));
				dependencyEntity.setDependencyFqdn(rs
						.getString(TableDependencyEntity.DEPENDENCY_FQDN));
				list.add(dependencyEntity);
			}
		} catch (SQLException e) {
			LOG.error("Could not query table dependencies", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return list;
	}

	@Override
	public void save(Connection connection, TableDependencyEntity dependency) {
		String insertDependencySql = "insert into table_dependency_entity ("
				+ JDBCUtil
						.getDatabaseColumnsForClass(TableDependencyEntity.class)
				+ ") values ("
				+ JDBCUtil.getValuesCountForClass(TableDependencyEntity.class)
				+ ")";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(insertDependencySql);
			stmt.setString(1, dependency.getFqdn());
			stmt.setString(2, dependency.getDependencyFqdn());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not save table dependency", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public void update(Connection connection, TableDependencyEntity dependency) {
		String updateDependencySql = "update table_dependency_entity set "
				+ JDBCUtil
						.getSetExpressionForClass(TableDependencyEntity.class)
				+ " where " + TableDependencyEntity.FQDN + " = ? and "
				+ TableDependencyEntity.DEPENDENCY_FQDN + " = ?";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(updateDependencySql);
			stmt.setString(1, dependency.getFqdn());
			stmt.setString(2, dependency.getDependencyFqdn());
			stmt.setString(3, dependency.getFqdn());
			stmt.setString(4, dependency.getDependencyFqdn());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not update table dependency", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

}
