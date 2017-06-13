package org.schedoscope.metascope.repository.jdbc.entity;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.MetascopeView;
import org.schedoscope.metascope.repository.jdbc.JDBCContext;
import org.schedoscope.metascope.task.model.Dependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JDBCMetascopeViewRepository extends JDBCContext {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCMetascopeViewRepository.class);

    public JDBCMetascopeViewRepository(boolean isMySQLDatabase, boolean isH2Database) {
        super(isMySQLDatabase, isH2Database);
    }

    public void insertOrUpdateViews(Connection connection, Iterable<MetascopeView> views) {
        String insertViewSql = "insert into metascope_view (view_id, view_url, parameter_string, table_fqdn) values "
          + "(?, ?, ?, ?) on duplicate key update view_id=values(view_id), view_url=values(view_url), "
          + "parameter_string=values(parameter_string), table_fqdn=values(table_fqdn)";
        PreparedStatement stmt = null;
        try {
            int batch = 0;
            disableChecks(connection);

            stmt = connection.prepareStatement(insertViewSql);
            for (MetascopeView viewEntity : views) {
                stmt.setString(1, viewEntity.getViewId());
                stmt.setString(2, viewEntity.getViewUrl());
                stmt.setString(3, viewEntity.getParameterString());
                stmt.setString(4, viewEntity.getTable().getFqdn());
                stmt.addBatch();
                batch++;
                if (batch % 10000 == 0) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();
            connection.commit();
            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not save view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void insertViewDependencies(Connection connection, List<Dependency> viewDependencies) {
        String sql = "insert into metascope_view_relationship (successor, dependency) values (?, ?) "
          + "on duplicate key update successor=values(successor), dependency=values(dependency)";
        PreparedStatement stmt = null;
        try {
            int batch = 0;
            disableChecks(connection);

            stmt = connection.prepareStatement(sql);
            for (Dependency viewDependency : viewDependencies) {
                stmt.setString(1, viewDependency.getDependency());
                stmt.setString(2, viewDependency.getSuccessor());
                stmt.addBatch();
                batch++;
                if (batch % 10000 == 0) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();
            connection.commit();
            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not save view", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void updateStatus(Connection connection, Iterable<MetascopeView> views) {
        String updateStatus = "update metascope_view set last_transformation=?, total_size=?, num_rows=? where view_id = ?";
        PreparedStatement updateStatusStmt = null;
        try {
            int batch = 0;
            disableChecks(connection);
            updateStatusStmt = connection.prepareStatement(updateStatus);
            for (MetascopeView viewEntity : views) {
                updateStatusStmt.setLong(1, viewEntity.getLastTransformation());
                updateStatusStmt.setLong(2, viewEntity.getTotalSize());
                updateStatusStmt.setLong(3, viewEntity.getNumRows());
                updateStatusStmt.setString(4, viewEntity.getViewId());
                updateStatusStmt.addBatch();
                batch++;
                if (batch % 1024 == 0) {
                    updateStatusStmt.executeBatch();
                }
            }
            updateStatusStmt.executeBatch();
            connection.commit();
            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not update view", e);
        } finally {
            DbUtils.closeQuietly(updateStatusStmt);
        }
    }
    
}
