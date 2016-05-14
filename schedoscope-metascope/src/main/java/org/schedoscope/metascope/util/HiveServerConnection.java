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
package org.schedoscope.metascope.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveServerConnection {

	private static final Logger LOG = LoggerFactory
			.getLogger(HiveServerConnection.class);

	private MetascopeConfig config;
	private Connection connection;

	public HiveServerConnection(MetascopeConfig config) {
		this.config = config;
	}

	public HiveServerConnection connect() {
		try {
			Class.forName(config.getHiveJdbcDriver());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		try {
			this.connection = DriverManager.getConnection(config
					.getHiveServerUrl());
		} catch (SQLException e) {
			LOG.warn("Could not connect to hive server", e);
		}
		return this;
	}

	public void close() {
		try {
			this.connection.close();
		} catch (SQLException e) {
			LOG.error("Failed closing connection to hive server", e);
		}
	}

	public Statement createStatement() throws SQLException {
		return this.connection.createStatement();
	}

	public Connection getConnection() {
		return connection;
	}

}
