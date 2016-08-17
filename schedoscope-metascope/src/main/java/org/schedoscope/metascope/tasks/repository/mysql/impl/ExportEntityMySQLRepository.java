package org.schedoscope.metascope.tasks.repository.mysql.impl;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ExportEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;

public class ExportEntityMySQLRepository implements MySQLRepository<ExportEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(ExportEntityMySQLRepository.class);

    @Override
    public void insertOrUpdate(Connection connection, ExportEntity exportEntity) {
        String insertExportSql = "insert into export_entity (" + JDBCUtil.getDatabaseColumnsForClass(ExportEntity.class)
                + ") values (" + JDBCUtil.getValuesCountForClass(ExportEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(ExportEntity.class);
        String insertExportPropertySql = "insert into export_entity_properties (export_entity_exportid, properties_key, properties)"
                + " values (?, ?, ?)";
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertExportSql);
            stmt.setString(1, exportEntity.getExportId());
            stmt.setString(2, exportEntity.getFqdn());
            stmt.setString(3, exportEntity.getType());
            stmt.execute();
            stmt.close();

            stmt = connection.prepareStatement(insertExportPropertySql);
            for (Entry<String, String> e : exportEntity.getProperties().entrySet()) {
                stmt.setString(1, exportEntity.getExportId());
                stmt.setString(2, e.getKey());
                stmt.setString(3, e.getValue());
                stmt.execute();
            }

        } catch (SQLException e) {
            LOG.error("Could not save transformation property", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void insertOrUpdate(Connection connection, List<ExportEntity> entities) {
        String insertExportSql = "insert into export_entity (" + JDBCUtil.getDatabaseColumnsForClass(ExportEntity.class)
                + ") values (" + JDBCUtil.getValuesCountForClass(ExportEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(ExportEntity.class);
        String insertExportPropertySql = "insert into export_entity_properties (export_entity_exportid, properties_key, properties)"
                + " values (?, ?, ?)";
        PreparedStatement stmt = null;
        try {
            int batch = 0;
            connection.setAutoCommit(false);
            stmt = connection.prepareStatement(insertExportSql);
            for (ExportEntity exportEntity : entities) {
                stmt.setString(1, exportEntity.getExportId());
                stmt.setString(2, exportEntity.getFqdn());
                stmt.setString(3, exportEntity.getType());
                stmt.addBatch();
                batch++;
                if (batch % 1024 == 0) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();

            batch = 0;
            stmt.close();

            stmt = connection.prepareStatement(insertExportPropertySql);
            for (ExportEntity exportEntity : entities) {
                for (Entry<String, String> e : exportEntity.getProperties().entrySet()) {
                    stmt.setString(1, exportEntity.getExportId());
                    stmt.setString(2, e.getKey());
                    stmt.setString(3, e.getValue());
                    stmt.addBatch();
                    batch++;
                    if (batch % 1024 == 0) {
                        stmt.executeBatch();
                    }
                }
            }
            stmt.executeBatch();

            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            LOG.error("Could not save transformation property", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

}
