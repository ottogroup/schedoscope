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
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.key.FieldEntityKey;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldEntityJdbcRepository implements
		JdbcRepository<FieldEntity, FieldEntityKey> {

	private static final Logger LOG = LoggerFactory
			.getLogger(FieldEntityJdbcRepository.class);

	@Override
	public FieldEntity get(Connection connection, FieldEntityKey key) {
		FieldEntity fieldEntity = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement("select "
					+ JDBCUtil.getDatabaseColumnsForClass(FieldEntity.class)
					+ " from field_entity where " + "fqdn = ? and name = ?");
			stmt.setString(1, key.getFqdn());
			stmt.setString(2, key.getName());
			rs = stmt.executeQuery();
			if (rs.next()) {
				fieldEntity = new FieldEntity();
				fieldEntity.setFqdn(rs.getString(FieldEntity.FQDN));
				fieldEntity.setName(rs.getString(FieldEntity.NAME));
				fieldEntity.setDescription(rs
						.getString(FieldEntity.DESCRIPTION));
				fieldEntity.setFieldOrder(rs.getInt(FieldEntity.FIELD_ORDER));
				fieldEntity.setParameterField(rs
						.getBoolean(FieldEntity.PARAMETER_FIELD));
				fieldEntity.setType(rs.getString(FieldEntity.TYPE));
				fieldEntity.setFqdn(rs.getString(FieldEntity.FQDN));
				fieldEntity.setName(rs.getString(FieldEntity.NAME));
			}
		} catch (SQLException e) {
			LOG.error("Could not query field", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return fieldEntity;
	}

	@Override
	public List<FieldEntity> get(Connection connection) {
		List<FieldEntity> list = new ArrayList<FieldEntity>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement("select "
					+ JDBCUtil.getDatabaseColumnsForClass(FieldEntity.class)
					+ " from field_entity");
			rs = stmt.executeQuery();
			while (rs.next()) {
				FieldEntity fieldEntity = new FieldEntity();
				fieldEntity.setFqdn(rs.getString(FieldEntity.FQDN));
				fieldEntity.setName(rs.getString(FieldEntity.NAME));
				fieldEntity.setDescription(rs
						.getString(FieldEntity.DESCRIPTION));
				fieldEntity.setFieldOrder(rs.getInt(FieldEntity.FIELD_ORDER));
				fieldEntity.setParameterField(rs
						.getBoolean(FieldEntity.PARAMETER_FIELD));
				fieldEntity.setType(rs.getString(FieldEntity.TYPE));
				list.add(fieldEntity);
			}
		} catch (SQLException e) {
			LOG.error("Could not query fields", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return list;
	}

	@Override
	public void save(Connection connection, FieldEntity fieldEntity) {
		String insertFieldSql = "insert into field_entity ("
				+ JDBCUtil.getDatabaseColumnsForClass(FieldEntity.class)
				+ ") values ("
				+ JDBCUtil.getValuesCountForClass(FieldEntity.class) + ")";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(insertFieldSql);
			stmt.setString(1, fieldEntity.getFqdn());
			stmt.setString(2, fieldEntity.getName());
			stmt.setString(3, fieldEntity.getType());
			stmt.setString(4, fieldEntity.getDescription());
			stmt.setInt(5, fieldEntity.getFieldOrder());
			stmt.setBoolean(6, fieldEntity.isParameterField());
			stmt.setString(7, fieldEntity.getFqdn());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not save field", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public void update(Connection connection, FieldEntity fieldEntity) {
		String updateFieldSql = "update field_entity set "
				+ JDBCUtil.getSetExpressionForClass(FieldEntity.class)
				+ " where " + FieldEntity.FQDN + " = ? and " + FieldEntity.NAME
				+ " = ?";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(updateFieldSql);
			stmt.setString(1, fieldEntity.getFqdn());
			stmt.setString(2, fieldEntity.getName());
			stmt.setString(3, fieldEntity.getType());
			stmt.setString(4, fieldEntity.getDescription());
			stmt.setInt(5, fieldEntity.getFieldOrder());
			stmt.setBoolean(6, fieldEntity.isParameterField());
			stmt.setString(7, fieldEntity.getFqdn());
			stmt.setString(8, fieldEntity.getFqdn());
			stmt.setString(9, fieldEntity.getName());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not update field", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

}
