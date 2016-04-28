/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.tasks.repository.mysql.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableDependencyEntityMySQLRepository implements MySQLRepository<TableDependencyEntity> {

  private static final Logger LOG = LoggerFactory.getLogger(TableDependencyEntityMySQLRepository.class);

  @Override
  public void insertOrUpdate(Connection connection, TableDependencyEntity dependency) {
    String insertDependencySql = "insert into table_dependency_entity ("
        + JDBCUtil.getDatabaseColumnsForClass(TableDependencyEntity.class) + ") values ("
        + JDBCUtil.getValuesCountForClass(TableDependencyEntity.class) + ") " + "on duplicate key update "
        + MySQLUtil.getOnDuplicateKeyString(TableDependencyEntity.class);
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(insertDependencySql);
      stmt.setString(1, dependency.getFqdn());
      stmt.setString(2, dependency.getDependencyFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save table dependency", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, List<TableDependencyEntity> tableDependencies) {
    String insertDependencySql = "insert into table_dependency_entity ("
        + JDBCUtil.getDatabaseColumnsForClass(TableDependencyEntity.class) + ") values ("
        + JDBCUtil.getValuesCountForClass(TableDependencyEntity.class) + ") " + "on duplicate key update "
        + MySQLUtil.getOnDuplicateKeyString(TableDependencyEntity.class);
    PreparedStatement stmt = null;
    try {
      int batch = 0;
      connection.setAutoCommit(false);
      stmt = connection.prepareStatement(insertDependencySql);
      for (TableDependencyEntity dependency : tableDependencies) {
        stmt.setString(1, dependency.getFqdn());
        stmt.setString(2, dependency.getDependencyFqdn());
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
      LOG.error("Could not save table dependency", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

}
