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
import org.schedoscope.metascope.model.TransformationEntity;
import org.schedoscope.metascope.model.key.TransformationEntityKey;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformationEntityJdbcRepository implements
    JdbcRepository<TransformationEntity, TransformationEntityKey> {

  private static final Logger LOG = LoggerFactory.getLogger(TransformationEntityJdbcRepository.class);

  @Override
  public TransformationEntity get(Connection connection, TransformationEntityKey key) {
    TransformationEntity transformationEntity = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(TransformationEntity.class)
          + " from transformation_entity where fqdn = ? and t_key = ?");
      stmt.setString(1, key.getFqdn());
      stmt.setString(2, key.getTransformationKey());
      rs = stmt.executeQuery();
      if (rs.next()) {
        transformationEntity = new TransformationEntity();
        transformationEntity.setFqdn(rs.getString(TransformationEntity.FQDN));
        transformationEntity.setTransformationKey(rs.getString(TransformationEntity.KEY));
        transformationEntity.setTransformationValue(rs.getString(TransformationEntity.VALUE));
      }
    } catch (SQLException e) {
      LOG.error("Could not query transformation properties", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return transformationEntity;
  }

  @Override
  public List<TransformationEntity> get(Connection connection) {
    List<TransformationEntity> list = new ArrayList<TransformationEntity>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(TransformationEntity.class)
          + " from transformation_entity");
      rs = stmt.executeQuery();
      while (rs.next()) {
        TransformationEntity transformationEntity = new TransformationEntity();
        transformationEntity.setFqdn(rs.getString(TransformationEntity.FQDN));
        transformationEntity.setTransformationKey(rs.getString(TransformationEntity.KEY));
        transformationEntity.setTransformationValue(rs.getString(TransformationEntity.VALUE));
        list.add(transformationEntity);
      }
    } catch (SQLException e) {
      LOG.error("Could not query transformation properties", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return list;
  }

  @Override
  public void save(Connection connection, TransformationEntity te) {
    String insertTeSql = "insert into transformation_entity ("
        + JDBCUtil.getDatabaseColumnsForClass(TransformationEntity.class) + ") values ("
        + JDBCUtil.getValuesCountForClass(TransformationEntity.class) + ")";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(insertTeSql);
      stmt.setString(1, te.getFqdn());
      stmt.setString(2, te.getTransformationKey());
      stmt.setString(3, te.getTransformationValue());
      stmt.setString(4, te.getFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save transformation property", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  @Override
  public void update(Connection connection, TransformationEntity te) {
    String updateTeSql = "update transformation_entity set "
        + JDBCUtil.getSetExpressionForClass(TransformationEntity.class) + " where " + TransformationEntity.FQDN
        + " = ? and " + TransformationEntity.KEY + "= ?";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(updateTeSql);
      stmt.setString(1, te.getFqdn());
      stmt.setString(2, te.getTransformationKey());
      stmt.setString(3, te.getTransformationValue());
      stmt.setString(4, te.getFqdn());
      stmt.setString(5, te.getFqdn());
      stmt.setString(6, te.getTransformationKey());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not update transformation property", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

}
