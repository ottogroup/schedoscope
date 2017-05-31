package org.schedoscope.metascope.repository.jdbc.entity;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.MetascopeExport;
import org.schedoscope.metascope.repository.jdbc.JDBCContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JDBCMetascopeExportRepository extends JDBCContext {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCMetascopeExportRepository.class);

    public JDBCMetascopeExportRepository(boolean isMySQLDatabase, boolean isH2Database) {
        super(isMySQLDatabase, isH2Database);
    }

    public MetascopeExport findExport(Connection connection, String exportFqdn) {
        MetascopeExport export = null;
        String findQuery = "select export_id, export_type, table_fqdn from metascope_export where export_id = ?";
        PreparedStatement stmt = null;
        PreparedStatement propertiesStmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.prepareStatement(findQuery);
            stmt.setString(1, exportFqdn);
            rs = stmt.executeQuery();
            if (rs.next()) {
                export = new MetascopeExport();
                export.setExportId(rs.getString("export_id"));
                export.setExportType(rs.getString("export_type"));
                export.setTableFqdn(rs.getString("table_fqdn"));

                propertiesStmt = connection.prepareStatement("select metascope_export_export_id, export_properties_key, " +
                  "export_properties from metascope_export_export_properties where metascope_export_export_id = ?");
                propertiesStmt.setString(1, export.getExportId());
                ResultSet propsRs = propertiesStmt.executeQuery();
                while (propsRs.next()) {
                    String key = propsRs.getString("export_properties_key");
                    String value = propsRs.getString("export_properties");
                    export.addProperty(key, value);
                }
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve table", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(propertiesStmt);
        }
        return export;
    }

    public void save(Connection connection, List<MetascopeExport> exports, String fqdn) {
        String deleteQuery = "delete from metascope_export where table_fqdn = ?";
        String deletePropertyQuery = "delete from metascope_export_export_properties where metascope_export_export_id = ?";
        String deleteMappingQuery = "delete from metascope_table_exports where metascope_table_fqdn = ?";
        String insertTableSql = "insert into metascope_export (export_id, export_type, table_fqdn) values "
          + "(?, ?, ?) on duplicate key update export_id=values(export_id), export_type=values(export_type), "
          + "table_fqdn=values(table_fqdn)";
        String insertPropsSql = "insert into metascope_export_export_properties (metascope_export_export_id, export_properties_key, " +
          "export_properties) values (?, ?, ?) on duplicate key update metascope_export_export_id=values(metascope_export_export_id), " +
          "export_properties_key=values(export_properties_key), export_properties=values(export_properties)";
        String insertMappingSql = "insert into metascope_table_exports (metascope_table_fqdn, exports_export_id) " +
                "values (?, ?) on duplicate key update metascope_table_fqdn=values(metascope_table_fqdn), " +
                "exports_export_id=values(exports_export_id)";
        PreparedStatement stmt = null;
        PreparedStatement propsStmt = null;
        PreparedStatement mappingStmt = null;
        PreparedStatement deleteStmt = null;
        PreparedStatement deletePropsStmt = null;
        PreparedStatement deleteMappingStmt = null;
        try {
            int batch = 0;
            disableChecks(connection);

            deleteStmt = connection.prepareStatement(deleteQuery);
            deleteStmt.setString(1, fqdn);
            deleteStmt.execute();

            for (MetascopeExport export : exports) {
                deletePropsStmt = connection.prepareStatement(deletePropertyQuery);
                deletePropsStmt.setString(1, export.getExportId());
                deletePropsStmt.execute();
                deletePropsStmt.close();
            }

            deleteMappingStmt = connection.prepareStatement(deleteMappingQuery);
            deleteMappingStmt.setString(1, fqdn);
            deleteMappingStmt.execute();

            stmt = connection.prepareStatement(insertTableSql);
            for (MetascopeExport export : exports) {
                stmt.setString(1, export.getExportId());
                stmt.setString(2, export.getExportType());
                stmt.setString(3, export.getTableFqdn());
                stmt.addBatch();

                batch++;
                if (batch % 1024 == 0) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();

            batch = 0;
            propsStmt = connection.prepareStatement(insertPropsSql);
            for (MetascopeExport export : exports) {
                for (Map.Entry<String, String> entry : export.getProperties().entrySet()) {
                    propsStmt.setString(1, export.getExportId());
                    propsStmt.setString(2, entry.getKey());
                    propsStmt.setString(3, entry.getValue());
                    propsStmt.addBatch();

                    batch++;
                    if (batch % 1024 == 0) {
                        propsStmt.executeBatch();
                    }
                }

            }
            propsStmt.executeBatch();

            batch = 0;
            mappingStmt = connection.prepareStatement(insertMappingSql);
            for (MetascopeExport export : exports) {
                mappingStmt.setString(1, fqdn);
                mappingStmt.setString(2, export.getExportId());
                mappingStmt.addBatch();

                batch++;
                if (batch % 1024 == 0) {
                    mappingStmt.executeBatch();
                }
            }
            mappingStmt.executeBatch();

            connection.commit();
            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not save/update exports", e);
        } finally {
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(propsStmt);
            DbUtils.closeQuietly(mappingStmt);
            DbUtils.closeQuietly(deleteStmt);
            DbUtils.closeQuietly(deletePropsStmt);
            DbUtils.closeQuietly(deleteMappingStmt);
        }
    }

}
