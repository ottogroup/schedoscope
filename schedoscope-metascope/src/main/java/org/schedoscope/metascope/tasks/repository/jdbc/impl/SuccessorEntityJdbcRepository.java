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
import org.schedoscope.metascope.model.SuccessorEntity;
import org.schedoscope.metascope.model.key.SuccessorEntityKey;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuccessorEntityJdbcRepository implements JdbcRepository<SuccessorEntity, SuccessorEntityKey> {

  private static final Logger LOG = LoggerFactory.getLogger(SuccessorEntityJdbcRepository.class);

  @Override
  public SuccessorEntity get(Connection connection, SuccessorEntityKey key) {
    SuccessorEntity successorEntity = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(SuccessorEntity.class)
          + " from successor_entity " + "where url_path = ? and successor_url_path = ? and successor_fqdn = ?");
      stmt.setString(1, key.getUrlPath());
      stmt.setString(2, key.getSuccessorUrlPath());
      stmt.setString(3, key.getSuccessorFqdn());
      rs = stmt.executeQuery();
      if (rs.next()) {
        successorEntity = new SuccessorEntity();
        successorEntity.setUrlPath(rs.getString(SuccessorEntity.URL_PATH));
        successorEntity.setSuccessorUrlPath(rs.getString(SuccessorEntity.SUCCESSOR_URL_PATH));
        successorEntity.setSuccessorFqdn(rs.getString(SuccessorEntity.SUCCESSOR_FQDN));
        successorEntity.setInternalViewId(rs.getString(SuccessorEntity.INTERNAL_VIEW_ID));
      }
    } catch (SQLException e) {
      LOG.error("Could not query successors", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return successorEntity;
  }

  @Override
  public List<SuccessorEntity> get(Connection connection) {
    List<SuccessorEntity> list = new ArrayList<SuccessorEntity>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(SuccessorEntity.class)
          + " from successor_entity");
      rs = stmt.executeQuery();
      while (rs.next()) {
        SuccessorEntity successorEntity = new SuccessorEntity();
        successorEntity.setUrlPath(rs.getString(SuccessorEntity.URL_PATH));
        successorEntity.setSuccessorUrlPath(rs.getString(SuccessorEntity.SUCCESSOR_URL_PATH));
        successorEntity.setSuccessorFqdn(rs.getString(SuccessorEntity.SUCCESSOR_FQDN));
        successorEntity.setInternalViewId(rs.getString(SuccessorEntity.INTERNAL_VIEW_ID));
        list.add(successorEntity);
      }
    } catch (SQLException e) {
      LOG.error("Could not query successors", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return list;
  }

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
  public void save(Connection connection, SuccessorEntity successorEntity) {
    String insertSuccessorSql = "insert into successor_entity ("
        + JDBCUtil.getDatabaseColumnsForClass(SuccessorEntity.class) + ") values ("
        + JDBCUtil.getValuesCountForClass(SuccessorEntity.class) + ")";
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
  public void update(Connection connection, SuccessorEntity successorEntity) {
    String updateSuccessorSql = "update successor_entity set "
        + JDBCUtil.getSetExpressionForClass(SuccessorEntity.class) + " where " + SuccessorEntity.URL_PATH + " = ? and "
        + SuccessorEntity.SUCCESSOR_URL_PATH + " = ? and " + SuccessorEntity.SUCCESSOR_FQDN + " = ?";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(updateSuccessorSql);
      stmt.setString(1, successorEntity.getUrlPath());
      stmt.setString(2, successorEntity.getSuccessorUrlPath());
      stmt.setString(3, successorEntity.getSuccessorFqdn());
      stmt.setString(4, successorEntity.getInternalViewId());
      stmt.setString(5, successorEntity.getUrlPath());
      stmt.setString(6, successorEntity.getSuccessorUrlPath());
      stmt.setString(7, successorEntity.getSuccessorFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not update successor", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

}
