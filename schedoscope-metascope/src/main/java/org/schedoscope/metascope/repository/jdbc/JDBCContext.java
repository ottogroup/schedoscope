package org.schedoscope.metascope.repository.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCContext {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCContext.class);

    private final boolean isMySQLDatabase;
    private final boolean isH2Database;

    public JDBCContext(boolean isMySQLDatabase, boolean isH2Database) {
        this.isMySQLDatabase = isMySQLDatabase;
        this.isH2Database = isH2Database;
    }

    protected void disableChecks(Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            connection.setAutoCommit(false);
            if (isMySQLDatabase) {
                stmt.addBatch("set foreign_key_checks=0");
                stmt.addBatch("set unique_checks=0");
            }
            if (isH2Database) {
                stmt.addBatch("SET REFERENTIAL_INTEGRITY FALSE");
            }
            stmt.executeBatch();
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Could not disable checks", e);
        }
    }

    protected void enableChecks(Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            connection.setAutoCommit(true);
            if (isMySQLDatabase) {
                stmt.addBatch("set foreign_key_checks=1");
                stmt.addBatch("set unique_checks=1");
            }
            if (isH2Database) {
                stmt.addBatch("SET REFERENTIAL_INTEGRITY TRUE");
            }
            stmt.executeBatch();
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Could not enable checks", e);
        }
    }

}
