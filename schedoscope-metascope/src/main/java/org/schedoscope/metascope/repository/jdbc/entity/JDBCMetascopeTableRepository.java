package org.schedoscope.metascope.repository.jdbc.entity;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeTransformation;
import org.schedoscope.metascope.repository.jdbc.JDBCContext;
import org.schedoscope.metascope.task.model.Dependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JDBCMetascopeTableRepository extends JDBCContext {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCMetascopeTableRepository.class);

    public JDBCMetascopeTableRepository(boolean isMySQLDatabase, boolean isH2Database) {
        super(isMySQLDatabase, isH2Database);
    }

    public MetascopeTable findTable(Connection connection, String fqdn) {
        MetascopeTable table = null;
        String findQuery = "select fqdn, schedoscope_id, database_name, table_name, view_path, "
          + "external_table, table_description, storage_format, input_format, output_format, materialize_once, created_at, "
          + "table_owner, data_path, data_size, permissions, rowcount, last_data, timestamp_field, timestamp_field_format, "
          + "last_change, last_partition_created, last_schema_change, last_transformation_timestamp, view_count, views_size, "
          + "person_responsible, comment_id "
          + "from metascope_table where fqdn = ?";
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(findQuery);
            stmt.setString(1, fqdn);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                table = new MetascopeTable();
                table.setFqdn(rs.getString("fqdn"));
                table.setSchedoscopeId(rs.getString("schedoscope_id"));
                table.setDatabaseName(rs.getString("database_name"));
                table.setTableName(rs.getString("table_name"));
                table.setViewPath(rs.getString("view_path"));
                table.setExternalTable(rs.getBoolean("external_table"));
                table.setTableDescription(rs.getString("table_description"));
                table.setStorageFormat(rs.getString("storage_format"));
                table.setInputFormat(rs.getString("input_format"));
                table.setOutputFormat(rs.getString("output_format"));
                table.setMaterializeOnce(rs.getBoolean("materialize_once"));
                table.setCreatedAt(rs.getLong("created_at"));
                table.setTableOwner(rs.getString("table_owner"));
                table.setDataPath(rs.getString("data_path"));
                table.setDataSize(rs.getLong("data_size"));
                table.setPermissions(rs.getString("permissions"));
                table.setRowcount(rs.getLong("rowcount"));
                table.setLastData(rs.getString("last_data"));
                table.setTimestampField(rs.getString("timestamp_field"));
                table.setTimestampFieldFormat(rs.getString("timestamp_field_format"));
                table.setLastChange(rs.getLong("last_change"));
                table.setLastPartitionCreated(rs.getLong("last_partition_created"));
                table.setLastSchemaChange(rs.getLong("last_schema_change"));
                table.setLastTransformation(rs.getLong("last_transformation_timestamp"));
                table.setViewCount(rs.getInt("view_count"));
                table.setViewsSize(rs.getInt("views_size"));
                table.setPersonResponsible(rs.getString("person_responsible"));
                long comment_id = rs.getLong("comment_id");
                table.setCommentId(rs.wasNull() ? null : comment_id);
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve table", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
        return table;
    }

    public void save(Connection connection, MetascopeTable table) {
        String insertTableSql = "insert into metascope_table (fqdn, schedoscope_id, database_name, table_name, view_path, "
          + "external_table, table_description, storage_format, input_format, output_format, materialize_once, created_at, "
          + "table_owner, data_path, data_size, permissions, rowcount, last_data, timestamp_field, timestamp_field_format, "
          + "last_change, last_partition_created, last_schema_change, last_transformation_timestamp, view_count, views_size, "
          + "person_responsible, comment_id) values "
          + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update "
          + "fqdn=values(fqdn), "
          + "schedoscope_id=values(schedoscope_id), "
          + "database_name=values(database_name), "
          + "table_name=values(table_name), "
          + "view_path=values(view_path), "
          + "external_table=values(external_table), "
          + "table_description=values(table_description), "
          + "storage_format=values(storage_format), "
          + "input_format=values(input_format), "
          + "output_format=values(output_format), "
          + "materialize_once=values(materialize_once), "
          + "created_at=values(created_at), "
          + "table_owner=values(table_owner), "
          + "data_path=values(data_path), "
          + "data_size=values(data_size), "
          + "permissions=values(permissions), "
          + "rowcount=values(rowcount), "
          + "last_data=values(last_data), "
          + "timestamp_field=values(timestamp_field), "
          + "timestamp_field_format=values(timestamp_field_format), "
          + "last_change=values(last_change), "
          + "last_partition_created=values(last_partition_created), "
          + "last_schema_change=values(last_schema_change), "
          + "last_transformation_timestamp=values(last_transformation_timestamp), "
          + "view_count=values(view_count), "
          + "views_size=values(views_size), "
          + "person_responsible=values(person_responsible), "
          + "comment_id=values(comment_id)";
        PreparedStatement stmt = null;
        try {
            disableChecks(connection);
            stmt = connection.prepareStatement(insertTableSql);
            stmt.setString(1, table.getFqdn());
            stmt.setString(2, table.getSchedoscopeId());
            stmt.setString(3, table.getDatabaseName());
            stmt.setString(4, table.getTableName());
            stmt.setString(5, table.getViewPath());
            stmt.setBoolean(6, table.isExternalTable());
            stmt.setString(7, table.getTableDescription());
            stmt.setString(8, table.getStorageFormat());
            stmt.setString(9, table.getInputFormat());
            stmt.setString(10, table.getOutputFormat());
            stmt.setBoolean(11, table.isMaterializeOnce());
            stmt.setLong(12, table.getCreatedAt());
            stmt.setString(13, table.getTableOwner());
            stmt.setString(14, table.getDataPath());
            stmt.setLong(15, table.getDataSize());
            stmt.setString(16, table.getPermissions());
            stmt.setLong(17, table.getRowcount());
            stmt.setString(18, table.getLastData());
            stmt.setString(19, table.getTimestampField());
            stmt.setString(20, table.getTimestampFieldFormat());
            stmt.setLong(21, table.getLastChange());
            stmt.setLong(22, table.getLastPartitionCreated());
            stmt.setLong(23, table.getLastSchemaChange());
            stmt.setLong(24, table.getLastTransformation());
            stmt.setInt(25, table.getViewCount());
            stmt.setInt(26, table.getViewsSize());
            stmt.setString(27, table.getPersonResponsible());
            if (table.getCommentId() == null) {
                stmt.setNull(28, Types.BIGINT);
            } else {
                stmt.setLong(28, table.getCommentId());
            }
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not save/update table", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    public void saveTransformation(Connection connection, MetascopeTransformation transformation, String fqdn) {
        String deleteQuery = "delete from metascope_transformation where table_fqdn = ?";
        String deletePropsQuery = "delete from metascope_transformation_properties where metascope_transformation_transformation_id = ?";
        String insertInto = "insert into metascope_transformation (transformation_id, transformation_type, table_fqdn) values " +
          "(?, ?, ?) on duplicate key update transformation_id=values(transformation_id), transformation_type=values(transformation_type)," +
          "table_fqdn=values(table_fqdn)";
        String insertIntoProps = "insert into metascope_transformation_properties (metascope_transformation_transformation_id, properties_key, properties) values " +
                "(?, ?, ?) on duplicate key update metascope_transformation_transformation_id=values(metascope_transformation_transformation_id), " +
                "properties_key=values(properties_key), properties=values(properties)";

        PreparedStatement stmt = null;
        PreparedStatement propsStmt = null;
        PreparedStatement delStmt = null;
        PreparedStatement delPropsStmt = null;
        try {
            disableChecks(connection);

            delStmt = connection.prepareStatement(deleteQuery);
            delStmt.setString(1, fqdn);
            delStmt.execute();

            delPropsStmt = connection.prepareStatement(deletePropsQuery);
            delPropsStmt.setString(1, transformation.getTransformationId());
            delPropsStmt.execute();

            stmt = connection.prepareStatement(insertInto);
            stmt.setString(1, transformation.getTransformationId());
            stmt.setString(2, transformation.getTransformationType());
            stmt.setString(3, fqdn);
            stmt.execute();

            for (Map.Entry<String, String> entry : transformation.getProperties().entrySet()) {
                propsStmt = connection.prepareStatement(insertIntoProps);
                propsStmt.setString(1, transformation.getTransformationId());
                propsStmt.setString(2, entry.getKey());
                propsStmt.setString(3, entry.getValue());
                propsStmt.execute();
            }

            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not save/update fields", e);
        } finally {
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(propsStmt);
            DbUtils.closeQuietly(delStmt);
            DbUtils.closeQuietly(delPropsStmt);
        }

    }

    public void saveTableDependency(Connection connection, Collection<MetascopeTable> currentTables, List<Dependency> tableDependencies) {
        String delSql = "delete from metascope_table_relationship where successor = ? or dependency = ?";
        String sql = "insert into metascope_table_relationship (successor, dependency) values (?, ?) "
          + "on duplicate key update successor=values(successor), dependency=values(dependency)";
        PreparedStatement stmt = null;
        PreparedStatement delStmt = null;
        try {
            int batch = 0;
            disableChecks(connection);

            delStmt = connection.prepareStatement(delSql);
            for (MetascopeTable t : currentTables) {
                delStmt.setString(1, t.getFqdn());
                delStmt.setString(2, t.getFqdn());
                delStmt.addBatch();
                batch++;
                if (batch % 1024 == 0) {
                    delStmt.executeBatch();
                }
            }
            delStmt.executeBatch();

            batch = 0;
            stmt = connection.prepareStatement(sql);
            for (Dependency tableDependency : tableDependencies) {
                stmt.setString(1, tableDependency.getDependency());
                stmt.setString(2, tableDependency.getSuccessor());
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
            LOG.error("Could not save table deoendencies", e);
        } finally {
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(delStmt);
        }
    }

}
