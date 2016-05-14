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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataMySQLRepository implements MySQLRepository<Metadata> {

	private static final Logger LOG = LoggerFactory
			.getLogger(MetadataMySQLRepository.class);

	public Metadata get(Connection connection, String key) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Metadata metadata = null;
		try {
			stmt = connection
					.prepareStatement("select * from metadata where metadata_key = ?");
			stmt.setString(1, key);
			rs = stmt.executeQuery();
			if (rs.next()) {
				metadata = new Metadata(key, rs.getString("metadata_value"));
			}
		} catch (SQLException e) {
			LOG.error("Could not get distinct parameters parameters", e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
		return metadata;
	}

	@Override
	public void insertOrUpdate(Connection connection, Metadata metadata) {
		String insertMetadataSql = "insert into metadata (metadata_key, metadata_value) values (?, ?) "
				+ "on duplicate key update metadata_key=values(metadata_key), metadata_value=values(metadata_value)";
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(insertMetadataSql);
			stmt.setString(1, metadata.getMetadataKey());
			stmt.setString(2, metadata.getMetadataValue());
			stmt.execute();
		} catch (SQLException e) {
			LOG.error("Could not save metadata", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public void insertOrUpdate(Connection connection, List<Metadata> entities) {
		throw new UnsupportedOperationException("Not implemented");
	}

}
