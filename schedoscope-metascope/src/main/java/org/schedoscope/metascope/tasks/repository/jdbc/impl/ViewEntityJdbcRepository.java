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
package org.schedoscope.metascope.tasks.repository.jdbc.impl;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ViewEntityJdbcRepository implements JdbcRepository<ViewEntity, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ViewEntityJdbcRepository.class);

    public ViewEntity get(Connection connection, String urlPath) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        ViewEntity viewEntity = null;
        try {
            stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(ViewEntity.class)
                    + " from view_entity where url_path = ?");
            stmt.setString(1, urlPath);
            rs = stmt.executeQuery();
            if (rs.next()) {
                viewEntity = new ViewEntity();
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
            }
        } catch (SQLException e) {
            LOG.error("Could not get view from repository", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(stmt);
        }
        return viewEntity;
    }

    @Override
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
    public void save(Connection connection, ViewEntity viewEntity) {
        String insertViewSql = "insert into view_entity (" + JDBCUtil.getDatabaseColumnsForClass(ViewEntity.class)
                + ") values (" + JDBCUtil.getValuesCountForClass(ViewEntity.class) + ")";
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

    public void savePartial(Connection connection, ViewEntity viewEntity) {
        String insertViewSql = "insert into view_entity (url_path, fqdn, status, parameter_string, internal_view_id, table_fqdn) values "
                + "(?, ?, ?, ?, ?, ?)";
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertViewSql);
            stmt.setString(1, viewEntity.getUrlPath());
            stmt.setString(2, viewEntity.getFqdn());
            stmt.setString(3, viewEntity.getStatus());
            stmt.setString(4, viewEntity.getParameterString());
            stmt.setString(5, viewEntity.getInternalViewId());
            stmt.setString(6, viewEntity.getFqdn());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not save view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    @Override
    public void update(Connection connection, ViewEntity viewEntity) {
        String updateViewSql = "update view_entity set " + JDBCUtil.getSetExpressionForClass(ViewEntity.class) + " where "
                + ViewEntity.URL_PATH + " = ?";
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(updateViewSql);
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
            stmt.setString(12, viewEntity.getUrlPath());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not update view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void updatePartial(Connection connection, ViewEntity viewEntity) {
        String insertViewSql = "update view_entity set url_path=?, fqdn=?, status=?, parameter_string=?, internal_view_id=? "
                + "where " + ViewEntity.URL_PATH + " = ?";
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertViewSql);
            stmt.setString(1, viewEntity.getUrlPath());
            stmt.setString(2, viewEntity.getFqdn());
            stmt.setString(3, viewEntity.getStatus());
            stmt.setString(4, viewEntity.getParameterString());
            stmt.setString(5, viewEntity.getInternalViewId());
            stmt.setString(6, viewEntity.getUrlPath());
            stmt.execute();
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

}
