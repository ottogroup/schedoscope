/**
 * Copyright 2015 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.jobs;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.HiveServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LastDataJob implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LastDataJob.class);

    private TableEntity tableEntity;
    private RepositoryDAO repo;
    private DataSource dataSource;
    private long start;
    private HiveServerConnection hiveServer;

    public LastDataJob(TableEntity tableEntity, RepositoryDAO repo, DataSource dataSource, MetascopeConfig config,
                       long start) {
        this.tableEntity = tableEntity;
        this.repo = repo;
        this.dataSource = dataSource;
        this.start = start;
        this.hiveServer = new HiveServerConnection(config);
    }

    @Override
    public void run() {
        this.hiveServer.connect();
        LOG.info("LastDataJob for table '{}' started", tableEntity.getFqdn());
        String fn = tableEntity.getTimestampField();
        String tn = tableEntity.getFqdn();
        String sql = "select max(" + fn + ") as ts from " + tn;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = hiveServer.createStatement();
            rs = stmt.executeQuery(sql);
            if (rs.next()) {
                tableEntity.setLastData(rs.getString("ts"));
                tableEntity.setLastChange(start);
            }
        } catch (SQLException e) {
            LOG.error("Could not query for last timestamp", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(hiveServer.getConnection());
        }

        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            LOG.error("Could not connect to repository", e);
            return;
        }

        repo.insertOrUpdate(connection, tableEntity);

        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("Could not close connection to repository", e);
        }

        LOG.info("LastDataJob for table '{}' finished", tableEntity.getFqdn());
    }

}
