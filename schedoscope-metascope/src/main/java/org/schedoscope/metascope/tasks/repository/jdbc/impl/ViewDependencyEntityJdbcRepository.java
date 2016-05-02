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
package org.schedoscope.metascope.tasks.repository.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ViewDependencyEntity;
import org.schedoscope.metascope.model.key.DependencyEntityKey;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewDependencyEntityJdbcRepository implements JdbcRepository<ViewDependencyEntity, DependencyEntityKey> {

  private static final Logger LOG = LoggerFactory.getLogger(ViewDependencyEntityJdbcRepository.class);

  @Override
  public ViewDependencyEntity get(Connection connection, DependencyEntityKey key) {
    ViewDependencyEntity dependencyEntity = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(ViewDependencyEntity.class)
          + " from view_dependency_entity where url_path = ? and dependency_url_path = ? and dependency_fqdn = ?");
      stmt.setString(1, key.getUrlPath());
      stmt.setString(2, key.getDependencyUrlPath());
      stmt.setString(3, key.getDependencyFqdn());
      rs = stmt.executeQuery();
      if (rs.next()) {
        dependencyEntity = new ViewDependencyEntity();
        dependencyEntity.setUrlPath(rs.getString(ViewDependencyEntity.URL_PATH));
        dependencyEntity.setDependencyUrlPath(rs.getString(ViewDependencyEntity.DEPENDENCY_URL_PATH));
        dependencyEntity.setDependencyFqdn(rs.getString(ViewDependencyEntity.DEPENDENCY_FQDN));
        dependencyEntity.setInternalViewId(rs.getString(ViewDependencyEntity.INTERNAL_VIEW_ID));
      }
    } catch (SQLException e) {
      LOG.error("Could not query view dependencies", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return dependencyEntity;
  }

  @Override
  public List<ViewDependencyEntity> get(Connection connection) {
    List<ViewDependencyEntity> list = new ArrayList<ViewDependencyEntity>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(ViewDependencyEntity.class)
          + " from view_dependency_entity");
      rs = stmt.executeQuery();
      while (rs.next()) {
        ViewDependencyEntity dependencyEntity = new ViewDependencyEntity();
        dependencyEntity.setUrlPath(rs.getString(ViewDependencyEntity.URL_PATH));
        dependencyEntity.setDependencyUrlPath(rs.getString(ViewDependencyEntity.DEPENDENCY_URL_PATH));
        dependencyEntity.setDependencyFqdn(rs.getString(ViewDependencyEntity.DEPENDENCY_FQDN));
        dependencyEntity.setInternalViewId(rs.getString(ViewDependencyEntity.INTERNAL_VIEW_ID));
        list.add(dependencyEntity);
      }
    } catch (SQLException e) {
      LOG.error("Could not query view dependencies", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return list;
  }

  @Override
  public void save(Connection connection, ViewDependencyEntity viewDependencyEntity) {
    String insertDependencySql = "insert into view_dependency_entity ("
        + JDBCUtil.getDatabaseColumnsForClass(ViewDependencyEntity.class) + ") values ("
        + JDBCUtil.getValuesCountForClass(ViewDependencyEntity.class) + ")";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(insertDependencySql);
      stmt.setString(1, viewDependencyEntity.getUrlPath());
      stmt.setString(2, viewDependencyEntity.getDependencyUrlPath());
      stmt.setString(3, viewDependencyEntity.getDependencyFqdn());
      stmt.setString(4, viewDependencyEntity.getInternalViewId());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save view dependency", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  @Override
  public void update(Connection connection, ViewDependencyEntity viewDependencyEntity) {
    String updateDependencySql = "update view_dependency_entity set "
        + JDBCUtil.getSetExpressionForClass(ViewDependencyEntity.class) + " where " + ViewDependencyEntity.URL_PATH
        + " = ? and " + ViewDependencyEntity.DEPENDENCY_URL_PATH + " = ? and " + ViewDependencyEntity.DEPENDENCY_FQDN
        + " = ?";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(updateDependencySql);
      stmt.setString(1, viewDependencyEntity.getUrlPath());
      stmt.setString(2, viewDependencyEntity.getDependencyUrlPath());
      stmt.setString(3, viewDependencyEntity.getDependencyFqdn());
      stmt.setString(4, viewDependencyEntity.getInternalViewId());
      stmt.setString(5, viewDependencyEntity.getUrlPath());
      stmt.setString(6, viewDependencyEntity.getDependencyUrlPath());
      stmt.setString(7, viewDependencyEntity.getDependencyFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not update view dependency", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

}
