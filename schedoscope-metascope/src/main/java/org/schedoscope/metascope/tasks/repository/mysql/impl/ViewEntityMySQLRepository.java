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
package org.schedoscope.metascope.tasks.repository.mysql.impl;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ViewEntityMySQLRepository implements MySQLRepository<ViewEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(ViewEntityMySQLRepository.class);

    public List<ViewEntity> get(Connection connection) {
        List<ViewEntity> list = new ArrayList<ViewEntity>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(ViewEntity.class)
                    + " from view_entity");
            rs = stmt.executeQuery();
            while (rs.next()) {
                ViewEntity viewEntity = new ViewEntity();
                viewEntity.setUrlPath(rs.getString(ViewEntity.URL_PATH));
                viewEntity.setFqdn(rs.getString(ViewEntity.FQDN));
                viewEntity.setStatus(rs.getString(ViewEntity.STATUS));
                viewEntity.setParameterString(rs.getString(ViewEntity.PARAMETERSTRING));
                viewEntity.setInternalViewId(rs.getString(ViewEntity.INTERNAL_VIEW_ID));
                viewEntity.setTransformationStart(rs.getLong(ViewEntity.TRANSFORMATION_START));
                viewEntity.setTransformationEnd(rs.getLong(ViewEntity.TRANSFORMATION_END));
                viewEntity.setCreatedAt(rs.getLong(ViewEntity.CREATED_AT));
                viewEntity.setRowJobFinished(rs.getBoolean(ViewEntity.ROW_JOB_FINISHED));
                viewEntity.setRows(rs.getLong(ViewEntity.ROWCOUNT));
                list.add(viewEntity);
            }
        } catch (SQLException e) {
            LOG.error("Could not query tables", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(stmt);
        }
        return list;
    }

    @Override
    public void insertOrUpdate(Connection connection, ViewEntity viewEntity) {
        String insertViewSql = "insert into view_entity (" + JDBCUtil.getDatabaseColumnsForClass(ViewEntity.class)
                + ") values (" + JDBCUtil.getValuesCountForClass(ViewEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(ViewEntity.class);
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertViewSql);
            stmt.setString(1, viewEntity.getUrlPath());
            stmt.setString(2, viewEntity.getFqdn());
            stmt.setString(3, viewEntity.getStatus());
            stmt.setString(4, viewEntity.getParameterString());
            stmt.setString(5, viewEntity.getInternalViewId());
            stmt.setLong(6, viewEntity.getTransformationStart());
            stmt.setLong(7, viewEntity.getTransformationEnd());
            stmt.setLong(8, viewEntity.getCreatedAt());
            stmt.setBoolean(9, viewEntity.isRowJobFinished());
            stmt.setLong(10, viewEntity.getRows());
            stmt.setString(11, viewEntity.getFqdn());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not save view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void insertOrUpdatePartial(Connection connection, ViewEntity viewEntity) {
        String insertViewSql = "insert into view_entity (url_path, fqdn, status, parameter_string, internal_view_id, table_fqdn) values "
                + "(?, ?, ?, ?, ?, ?) on duplicate key update url_path=values(url_path), fqdn=values(fqdn), "
                + "status=values(status), parameter_string=values(parameter_string), internal_view_id=values(internal_view_id), "
                + "table_fqdn=values(table_fqdn)";
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertViewSql);
            stmt.setString(1, viewEntity.getUrlPath());
            stmt.setString(2, viewEntity.getFqdn());
            stmt.setString(3, viewEntity.getStatus());
            stmt.setString(4, viewEntity.getParameterString());
            stmt.setString(5, viewEntity.getInternalViewId());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not save view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void insertOrUpdatePartial(Connection connection, Iterable<ViewEntity> views) {
        String insertViewSql = "insert into view_entity (url_path, fqdn, status, parameter_string, internal_view_id, table_fqdn) values "
                + "(?, ?, ?, ?, ?, ?) on duplicate key update url_path=values(url_path), fqdn=values(fqdn), "
                + "status=values(status), parameter_string=values(parameter_string), internal_view_id=values(internal_view_id), "
                + "table_fqdn=values(table_fqdn)";
        PreparedStatement stmt = null;
        try {
            int batch = 0;
            connection.setAutoCommit(false);
            stmt = connection.prepareStatement(insertViewSql);
            for (ViewEntity viewEntity : views) {
                stmt.setString(1, viewEntity.getUrlPath());
                stmt.setString(2, viewEntity.getFqdn());
                stmt.setString(3, viewEntity.getStatus());
                stmt.setString(4, viewEntity.getParameterString());
                stmt.setString(5, viewEntity.getInternalViewId());
                stmt.setString(6, viewEntity.getFqdn());
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
            LOG.error("Could not save view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void updateStatus(Connection connection, ViewEntity viewEntity, Long start, Long end) {
        String updateViewStatusSql = "update view_entity set " + ViewEntity.STATUS + " = ?";
        if (start != null) {
            updateViewStatusSql += ", " + ViewEntity.TRANSFORMATION_START + " = ?";
        }
        if (end != null) {
            updateViewStatusSql += ", " + ViewEntity.TRANSFORMATION_END + " = ?";
        }
        updateViewStatusSql += " where " + ViewEntity.URL_PATH + " = ?";

        PreparedStatement stmt = null;
        int i = 1;
        try {
            stmt = connection.prepareStatement(updateViewStatusSql);
            stmt.setString(i++, viewEntity.getStatus());
            if (start != null) {
                stmt.setLong(i++, start);
            }
            if (end != null) {
                stmt.setLong(i++, end);
            }
            stmt.setString(i, viewEntity.getUrlPath());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not update view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void updateStatus(Connection connection, Set<ViewEntity> views) {
        String updateStatus = "update view_entity set status=?, transformation_start=?, transformation_end=? where url_path = ?";
        PreparedStatement updateStatusStmt = null;
        try {
            int updateStatusBatch = 0;
            connection.setAutoCommit(false);
            updateStatusStmt = connection.prepareStatement(updateStatus);
            for (ViewEntity viewEntity : views) {
                updateStatusStmt.setString(1, viewEntity.getStatus());
                updateStatusStmt.setLong(2, viewEntity.getTransformationStart());
                updateStatusStmt.setLong(3, viewEntity.getTransformationEnd());
                updateStatusStmt.setString(4, viewEntity.getUrlPath());
                updateStatusStmt.addBatch();
                updateStatusBatch++;
                if (updateStatusBatch % 1000 == 0) {
                    updateStatusStmt.executeBatch();
                }
            }
            updateStatusStmt.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            LOG.error("Could not update view", e);
        } finally {
            DbUtils.closeQuietly(updateStatusStmt);
        }
    }

    @Override
    public void insertOrUpdate(Connection connection, List<ViewEntity> entities) {
        throw new UnsupportedOperationException("Not implemented");
    }

}
