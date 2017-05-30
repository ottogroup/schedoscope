package org.schedoscope.metascope.repository.jdbc.entity;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.repository.jdbc.JDBCContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCMetascopeMetadataRepository extends JDBCContext {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCMetascopeMetadataRepository.class);

    public JDBCMetascopeMetadataRepository(boolean isMySQLDatabase, boolean isH2Database) {
        super(isMySQLDatabase, isH2Database);
    }

    public void saveMetadata(Connection connection, String key, String value) {
        String insertTableSql = "insert into metascope_metadata (metadata_key, metadata_value) values "
          + "(?, ?) on duplicate key update metadata_key=values(metadata_key), metadata_value=values(metadata_value)";
        PreparedStatement stmt = null;
        try {
            disableChecks(connection);
            stmt = connection.prepareStatement(insertTableSql);
            stmt.setString(1, key);
            stmt.setString(2, value);
            stmt.execute();
            enableChecks(connection);
        } catch (SQLException e) {
            LOG.error("Could not save/update metadata", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

}
