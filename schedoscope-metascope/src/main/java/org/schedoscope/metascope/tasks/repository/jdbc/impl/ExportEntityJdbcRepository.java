package org.schedoscope.metascope.tasks.repository.jdbc.impl;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ExportEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;

public class ExportEntityJdbcRepository implements JdbcRepository<ExportEntity, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ExportEntityJdbcRepository.class);

    @Override
    public ExportEntity get(Connection connection, String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<ExportEntity> get(Connection connection) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void save(Connection connection, ExportEntity exportEntity) {
        String insertExportSql = "insert into export_entity (" + JDBCUtil.getDatabaseColumnsForClass(ExportEntity.class)
                + ") values (" + JDBCUtil.getValuesCountForClass(ExportEntity.class) + ")";
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

    @Override
    public void update(Connection connection, ExportEntity entity) {
        throw new UnsupportedOperationException("Not implemented");
    }

}
