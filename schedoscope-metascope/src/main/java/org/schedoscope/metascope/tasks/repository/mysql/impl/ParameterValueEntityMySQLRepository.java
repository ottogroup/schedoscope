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
import org.schedoscope.metascope.model.ParameterValueEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class ParameterValueEntityMySQLRepository implements MySQLRepository<ParameterValueEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(ParameterValueEntityMySQLRepository.class);

    @Override
    public void insertOrUpdate(Connection connection, ParameterValueEntity parameterValueEntity) {
        String insertParameterSql = "insert into parameter_value_entity ("
                + JDBCUtil.getDatabaseColumnsForClass(ParameterValueEntity.class) + ") values ("
                + JDBCUtil.getValuesCountForClass(ParameterValueEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(ParameterValueEntity.class);
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(insertParameterSql);
            stmt.setString(1, parameterValueEntity.getUrlPath());
            stmt.setString(2, parameterValueEntity.getKey());
            stmt.setString(3, parameterValueEntity.getValue());
            stmt.setString(4, parameterValueEntity.getTableFqdn());
            stmt.execute();
        } catch (SQLException e) {
            LOG.error("Could not save parameter value", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

    @Override
    public void insertOrUpdate(Connection connection, List<ParameterValueEntity> parameterValues) {
        String insertParameterSql = "insert into parameter_value_entity ("
                + JDBCUtil.getDatabaseColumnsForClass(ParameterValueEntity.class) + ") values ("
                + JDBCUtil.getValuesCountForClass(ParameterValueEntity.class) + ") " + "on duplicate key update "
                + MySQLUtil.getOnDuplicateKeyString(ParameterValueEntity.class);
        PreparedStatement stmt = null;
        try {
            int batch = 0;
            connection.setAutoCommit(false);
            stmt = connection.prepareStatement(insertParameterSql);
            for (ParameterValueEntity parameterValueEntity : parameterValues) {
                stmt.setString(1, parameterValueEntity.getUrlPath());
                stmt.setString(2, parameterValueEntity.getKey());
                stmt.setString(3, parameterValueEntity.getValue());
                stmt.setString(4, parameterValueEntity.getTableFqdn());
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
            LOG.error("Could not save parameter value", e);
        } finally {
            DbUtils.closeQuietly(stmt);
        }
    }

}
