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
import org.schedoscope.metascope.model.ParameterValueEntity;
import org.schedoscope.metascope.model.key.ParameterValueEntityKey;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterValueEntityJdbcRepository implements
		JdbcRepository<ParameterValueEntity, ParameterValueEntityKey> {

	private static final Logger LOG = LoggerFactory
			.getLogger(ParameterValueEntityJdbcRepository.class);

	@Override
	public ParameterValueEntity get(Connection connection,
			ParameterValueEntityKey key) {
		ParameterValueEntity parameterValueEntity = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection
					.prepareStatement("select "
							+ JDBCUtil
									.getDatabaseColumnsForClass(ParameterValueEntity.class)
							+ " from parameter_value_entity where url_path = ? and p_key = ?");
			stmt.setString(1, key.getUrlPath());
			stmt.setString(2, key.getKey());
			rs = stmt.executeQuery();
			if (rs.next()) {
				parameterValueEntity = new ParameterValueEntity();
				parameterValueEntity.setUrlPath(rs
						.getString(ParameterValueEntity.URL_PATH));
				parameterValueEntity.setKey(rs
						.getString(ParameterValueEntity.KEY));
				parameterValueEntity.setValue(rs
						.getString(ParameterValueEntity.VALUE));
				parameterValueEntity.setTableFqdn(rs
						.getString(ParameterValueEntity.TABLE_FQDN));
			}
		} catch (SQLException e) {
			LOG.error("Could not query parameter values", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return parameterValueEntity;
	}

	@Override
	public List<ParameterValueEntity> get(Connection connection) {
		List<ParameterValueEntity> list = new ArrayList<ParameterValueEntity>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection
					.prepareStatement("select "
							+ JDBCUtil
									.getDatabaseColumnsForClass(ParameterValueEntity.class)
							+ " from parameter_value_entity");
			rs = stmt.executeQuery();
			while (rs.next()) {
				ParameterValueEntity parameterValueEntity = new ParameterValueEntity();
				parameterValueEntity.setUrlPath(rs
						.getString(ParameterValueEntity.URL_PATH));
				parameterValueEntity.setKey(rs
						.getString(ParameterValueEntity.KEY));
				parameterValueEntity.setValue(rs
						.getString(ParameterValueEntity.VALUE));
				parameterValueEntity.setTableFqdn(rs
						.getString(ParameterValueEntity.TABLE_FQDN));
				list.add(parameterValueEntity);
			}
		} catch (SQLException e) {
			LOG.error("Could not query parameter values", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return list;
	}

	@Override
	public void save(Connection connection,
			ParameterValueEntity parameterValueEntity) {
		String insertParameterSql = "insert into parameter_value_entity ("
				+ JDBCUtil
						.getDatabaseColumnsForClass(ParameterValueEntity.class)
				+ ") values ("
				+ JDBCUtil.getValuesCountForClass(ParameterValueEntity.class)
				+ ")";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(insertParameterSql);
			stmt.setString(1, parameterValueEntity.getUrlPath());
			stmt.setString(2, parameterValueEntity.getKey());
			stmt.setString(3, parameterValueEntity.getValue());
			stmt.setString(4, parameterValueEntity.getTableFqdn());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not save parameter value", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public void update(Connection connection,
			ParameterValueEntity parameterValueEntity) {
		String updateParameterSql = "update parameter_value_entity set "
				+ JDBCUtil.getSetExpressionForClass(ParameterValueEntity.class)
				+ " where " + ParameterValueEntity.URL_PATH + " = ? and "
				+ ParameterValueEntity.KEY + " = ?";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(updateParameterSql);
			stmt.setString(1, parameterValueEntity.getUrlPath());
			stmt.setString(2, parameterValueEntity.getKey());
			stmt.setString(3, parameterValueEntity.getValue());
			stmt.setString(4, parameterValueEntity.getTableFqdn());
			stmt.setString(5, parameterValueEntity.getUrlPath());
			stmt.setString(6, parameterValueEntity.getKey());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not update parameter value", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

}
