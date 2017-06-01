package org.schedoscope.metascope.repository.jdbc.entity;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.repository.jdbc.JDBCContext;
import org.schedoscope.metascope.task.model.Dependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Set;

public class JDBCMetascopeFieldRepository extends JDBCContext {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCMetascopeFieldRepository.class);

    private static final String FIELD_MAPPING_TABLE = "metascope_fields_mapping";
    private static final String PARAMETER_MAPPING_TABLE = "metascope_parameter_mapping";
    private static final String FIELD_MAPPING_FIELD = "fields_field_id";
    private static final String PARAMETER_MAPPING_FIELD = "parameters_field_id";

    public JDBCMetascopeFieldRepository(boolean isMySQLDatabase, boolean isH2Database) {
        super(isMySQLDatabase, isH2Database);
    }

    public MetascopeField findField(Connection connection, String fieldFqdn) {
        MetascopeField field = null;
        String findQuery = "select field_id, field_name, field_type, field_order, is_parameter, description, comment_id, table_fqdn "
          + "from metascope_field where field_id = ?";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.prepareStatement(findQuery);
            stmt.setString(1, fieldFqdn);
            rs = stmt.executeQuery();
            if (rs.next()) {
                field = new MetascopeField();
                field.setFieldId(rs.getString("field_id"));
                field.setFieldName(rs.getString("field_name"));
                field.setFieldType(rs.getString("field_type"));
                field.setFieldOrder(rs.getInt("field_order"));
                field.setParameter(rs.getBoolean("is_parameter"));
                field.setDescription(rs.getString("description"));
                field.setTableFqdn(rs.getString("table_fqdn"));
                long commentId = rs.getLong("comment_id");
                field.setCommentId(rs.wasNull() ? null : commentId);
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve table", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(stmt);
        }
        return field;
    }

    public void saveFields(Connection connection, Set<MetascopeField> fields, String fqdn, boolean isParameter) {
        String mappingTable = isParameter ? PARAMETER_MAPPING_TABLE : FIELD_MAPPING_TABLE;
        String mappingField = isParameter ? PARAMETER_MAPPING_FIELD : FIELD_MAPPING_FIELD;

        String deleteQuery = "delete from metascope_field where table_fqdn = ? and is_parameter = ?";
        String deleteFromMappingTable = "delete from " + mappingTable + " where metascope_table_fqdn = ?";
        String insertIntoMetascopeField = "insert into metascope_field (field_id, field_name, field_type, field_order, "
          + "is_parameter, description, comment_id, table_fqdn) values "
          + "(?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update field_id=values(field_id), field_name=values(field_name), "
          + "field_type=values(field_type), field_order=values(field_order), is_parameter=values(is_parameter), "
          + "description=values(description), comment_id=values(comment_id), table_fqdn=values(table_fqdn)";
        String insertIntoMappingTable = "insert into " + mappingTable + " (metascope_table_fqdn, " + mappingField + ") values "
          + "(?,?) on duplicate key update metascope_table_fqdn=values(metascope_table_fqdn), " + mappingField + "=values(" + mappingField + ")";
        PreparedStatement insertMain = null;
        PreparedStatement insertMapping = null;
        PreparedStatement deleteStmt = null;
        PreparedStatement deleteMappingStmt = null;
        try {
            int batch = 0;
            disableChecks(connection);

            deleteStmt = connection.prepareStatement(deleteQuery);
            deleteStmt.setString(1, fqdn);
            deleteStmt.setBoolean(2, isParameter);
            deleteStmt.executeUpdate();
            deleteStmt.close();

            deleteMappingStmt = connection.prepareStatement(deleteFromMappingTable);
            deleteMappingStmt.setString(1, fqdn);
            deleteMappingStmt.execute();
            deleteMappingStmt.close();

            insertMain = connection.prepareStatement(insertIntoMetascopeField);
            insertMapping = connection.prepareStatement(insertIntoMappingTable);
            for (MetascopeField field : fields) {
                insertMain.setString(1, field.getFieldId());
                insertMain.setString(2, field.getFieldName());
                insertMain.setString(3, field.getFieldType());
                insertMain.setInt(4, field.getFieldOrder());
                insertMain.setBoolean(5, field.isParameter());
                insertMain.setString(6, field.getDescription());
                if (field.getCommentId() == null) {
                    insertMain.setNull(7, Types.BIGINT);
                } else {
                    insertMain.setLong(7, field.getCommentId());
                }
                insertMain.setString(8, field.getTableFqdn());
                insertMain.addBatch();

                insertMapping.setString(1, field.getTableFqdn());
                insertMapping.setString(2, field.getFieldId());
                insertMapping.addBatch();
                batch++;
                if (batch % 1024 == 0) {
                    insertMain.executeBatch();
                    insertMapping.executeBatch();
                }
            }
            insertMain.executeBatch();
            insertMapping.executeBatch();
            connection.commit();
            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not save/update fields", e);
        } finally {
            DbUtils.closeQuietly(insertMain);
            DbUtils.closeQuietly(insertMapping);
            DbUtils.closeQuietly(deleteStmt);
            DbUtils.closeQuietly(deleteMappingStmt);
        }
    }

    public void insertFieldDependencies(Connection connection, List<Dependency> fieldDependencies) {
        String deleteQuery = "delete from metascope_field_relationship";
        String sql = "insert into metascope_field_relationship (successor, dependency) values (?, ?) "
          + "on duplicate key update successor=values(successor), dependency=values(dependency)";
        PreparedStatement stmt = null;
        Statement delStmt = null;
        try {
            int batch = 0;
            disableChecks(connection);

            delStmt = connection.createStatement();
            delStmt.execute(deleteQuery);

            stmt = connection.prepareStatement(sql);
            for (Dependency fieldDependency : fieldDependencies) {
                stmt.setString(1, fieldDependency.getDependency());
                stmt.setString(2, fieldDependency.getSuccessor());
                stmt.addBatch();
                batch++;
                if (batch % 1024 == 0) {
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
            DbUtils.closeQuietly(delStmt);
        }
    }

}
