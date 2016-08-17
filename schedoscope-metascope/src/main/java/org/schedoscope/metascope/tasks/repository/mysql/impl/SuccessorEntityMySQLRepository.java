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
import org.schedoscope.metascope.model.SuccessorEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SuccessorEntityMySQLRepository implements MySQLRepository<SuccessorEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(SuccessorEntityMySQLRepository.class);

    public SuccessorEntity get(Connection connection, SuccessorEntity successorEntity) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        SuccessorEntity successor = null;
        try {
            stmt = connection.prepareStatement("select * from successor_entity where " + SuccessorEntity.URL_PATH
                    + " = ? and " + SuccessorEntity.SUCCESSOR_URL_PATH + " = ? and " + SuccessorEntity.SUCCESSOR_FQDN + " = ?");
            stmt.setString(1, successorEntity.getUrlPath());
            stmt.setString(2, successorEntity.getSuccessorUrlPath());
            stmt.setString(3, successorEntity.getSuccessorFqdn());
        } catch (SQLException e) {
            LOG.error("Could not get successor", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(stmt);
        }
        return successor;
    }

    @Override
    public void insertOrUpdate(Connection connection, SuccessorEntity successorEntity) {
        String insertSuccessorSql = "insert into successor_entity ("
                + JDBCUtil.getDatabaseColumnsForClass(SuccessorEntity.class) + ") values ("
                + JDBCUtil.getValuesCountForClass(SuccessorEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(SuccessorEntity.class);
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertSuccessorSql);
            stmt.setString(1, successorEntity.getUrlPath());
            stmt.setString(2, successorEntity.getSuccessorUrlPath());
            stmt.setString(3, successorEntity.getSuccessorFqdn());
            stmt.setString(4, successorEntity.getInternalViewId());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not save successor", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    @Override
    public void insertOrUpdate(Connection connection, List<SuccessorEntity> successors) {
        String insertSuccessorSql = "insert into successor_entity ("
                + JDBCUtil.getDatabaseColumnsForClass(SuccessorEntity.class) + ") values ("
                + JDBCUtil.getValuesCountForClass(SuccessorEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(SuccessorEntity.class);
        PreparedStatement stmt = null;
        try {
            int batch = 0;
            connection.setAutoCommit(false);
            stmt = connection.prepareStatement(insertSuccessorSql);
            for (SuccessorEntity successorEntity : successors) {
                stmt.setString(1, successorEntity.getUrlPath());
                stmt.setString(2, successorEntity.getSuccessorUrlPath());
                stmt.setString(3, successorEntity.getSuccessorFqdn());
                stmt.setString(4, successorEntity.getInternalViewId());
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
            LOG.error("Could not save successor", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

}
