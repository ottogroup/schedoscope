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
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableEntityJdbcRepository implements JdbcRepository<TableEntity, String> {

  private static final Logger LOG = LoggerFactory.getLogger(TableEntityJdbcRepository.class);

  public TableEntity get(Connection connection, String key) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    TableEntity tableEntity = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(TableEntity.class)
          + " from table_entity where table_fqdn = ?");
      stmt.setString(1, key);
      rs = stmt.executeQuery();
      if (rs.next()) {
        tableEntity = new TableEntity();
        tableEntity.setFqdn(rs.getString(TableEntity.TABLE_FQDN));
        tableEntity.setTableName(rs.getString(TableEntity.TABLE_NAME));
        tableEntity.setDatabaseName(rs.getString(TableEntity.DATABASE_NAME));
        tableEntity.setUrlPathPrefix(rs.getString(TableEntity.URL_PATH_PREFIX));
        tableEntity.setExternalTable(rs.getBoolean(TableEntity.EXTERNAL_TABLE));
        tableEntity.setTableDescription(rs.getString(TableEntity.TABLE_DESCRIPTION));
        tableEntity.setStorageFormat(rs.getString(TableEntity.STORAGE_FORMAT));
        tableEntity.setInputFormat(rs.getString(TableEntity.INPUT_FORMAT));
        tableEntity.setOutputFormat(rs.getString(TableEntity.OUTPUT_FORMAT));
        tableEntity.setMaterializeOnce(rs.getBoolean(TableEntity.MATERIALIZE_ONCE));
        tableEntity.setCreatedAt(rs.getLong(TableEntity.CREATED_AT));
        tableEntity.setStatus(rs.getString(TableEntity.STATUS));
        tableEntity.setTableOwner(rs.getString(TableEntity.TABLE_OWNER));
        tableEntity.setDataPath(rs.getString(TableEntity.DATA_PATH));
        tableEntity.setDataSize(rs.getLong(TableEntity.DATA_SIZE));
        tableEntity.setPermissions(rs.getString(TableEntity.PERMISSIONS));
        tableEntity.setRowcount(rs.getLong(TableEntity.ROWCOUNT));
        tableEntity.setTransformationType(rs.getString(TableEntity.TRANSFORMATION_TYPE));
        tableEntity.setLastData(rs.getString(TableEntity.LAST_DATA));
        tableEntity.setTimestampField(rs.getString(TableEntity.TIMESTAMP_FIELD));
        tableEntity.setTimestampFieldFormat(rs.getString(TableEntity.TIMESTAMP_FIELD_FORMAT));
        tableEntity.setLastChange(rs.getLong(TableEntity.LAST_CHANGE));
        tableEntity.setLastPartitionCreated(rs.getLong(TableEntity.LAST_PARTITION_CREATED));
        tableEntity.setLastSchemaChange(rs.getLong(TableEntity.LAST_SCHEMA_CHANGE));
        tableEntity.setLastTransformation(rs.getLong(TableEntity.LAST_TRANSFORMATION_TIMESTAMP));
      }
    } catch (SQLException e) {
      LOG.error("Could not get table from repository", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return tableEntity;
  }

  @Override
  public List<TableEntity> get(Connection connection) {
    List<TableEntity> list = new ArrayList<TableEntity>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.prepareStatement("select " + JDBCUtil.getDatabaseColumnsForClass(TableEntity.class)
          + " from table_entity");
      rs = stmt.executeQuery();
      while (rs.next()) {
        TableEntity tableEntity = new TableEntity();
        tableEntity.setFqdn(rs.getString(TableEntity.TABLE_FQDN));
        tableEntity.setTableName(rs.getString(TableEntity.TABLE_NAME));
        tableEntity.setDatabaseName(rs.getString(TableEntity.DATABASE_NAME));
        tableEntity.setUrlPathPrefix(rs.getString(TableEntity.URL_PATH_PREFIX));
        tableEntity.setExternalTable(rs.getBoolean(TableEntity.EXTERNAL_TABLE));
        tableEntity.setTableDescription(rs.getString(TableEntity.TABLE_DESCRIPTION));
        tableEntity.setStorageFormat(rs.getString(TableEntity.STORAGE_FORMAT));
        tableEntity.setInputFormat(rs.getString(TableEntity.INPUT_FORMAT));
        tableEntity.setOutputFormat(rs.getString(TableEntity.OUTPUT_FORMAT));
        tableEntity.setMaterializeOnce(rs.getBoolean(TableEntity.MATERIALIZE_ONCE));
        tableEntity.setCreatedAt(rs.getLong(TableEntity.CREATED_AT));
        tableEntity.setStatus(rs.getString(TableEntity.STATUS));
        tableEntity.setTableOwner(rs.getString(TableEntity.TABLE_OWNER));
        tableEntity.setDataPath(rs.getString(TableEntity.DATA_PATH));
        tableEntity.setDataSize(rs.getLong(TableEntity.DATA_SIZE));
        tableEntity.setPermissions(rs.getString(TableEntity.PERMISSIONS));
        tableEntity.setRowcount(rs.getLong(TableEntity.ROWCOUNT));
        tableEntity.setTransformationType(rs.getString(TableEntity.TRANSFORMATION_TYPE));
        tableEntity.setLastData(rs.getString(TableEntity.LAST_DATA));
        tableEntity.setTimestampField(rs.getString(TableEntity.TIMESTAMP_FIELD));
        tableEntity.setTimestampFieldFormat(rs.getString(TableEntity.TIMESTAMP_FIELD_FORMAT));
        tableEntity.setLastChange(rs.getLong(TableEntity.LAST_CHANGE));
        tableEntity.setLastPartitionCreated(rs.getLong(TableEntity.LAST_PARTITION_CREATED));
        tableEntity.setLastSchemaChange(rs.getLong(TableEntity.LAST_SCHEMA_CHANGE));
        tableEntity.setLastTransformation(rs.getLong(TableEntity.LAST_TRANSFORMATION_TIMESTAMP));
        list.add(tableEntity);
      }
    } catch (SQLException e) {
      LOG.error("Could not query tables", e);
    } finally {
      DbUtils.closeQuietly(rs);
      DbUtils.closeQuietly(stmt);
    }
    return list;
  }

  @Override
  public void save(Connection connection, TableEntity tableEntity) {
    String insertTableSql = "insert into table_entity (" + JDBCUtil.getDatabaseColumnsForClass(TableEntity.class)
        + ") values (" + JDBCUtil.getValuesCountForClass(TableEntity.class) + ")";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(insertTableSql);
      stmt.setString(1, tableEntity.getFqdn());
      stmt.setString(2, tableEntity.getTableName());
      stmt.setString(3, tableEntity.getDatabaseName());
      stmt.setString(4, tableEntity.getUrlPathPrefix());
      stmt.setBoolean(5, tableEntity.isExternalTable());
      stmt.setString(6, tableEntity.getTableDescription());
      stmt.setString(7, tableEntity.getStorageFormat());
      stmt.setString(8, tableEntity.getInputFormat());
      stmt.setString(9, tableEntity.getOutputFormat());
      stmt.setBoolean(10, tableEntity.isMaterializeOnce());
      stmt.setLong(11, tableEntity.getCreatedAt());
      stmt.setString(12, tableEntity.getStatus());
      stmt.setString(13, tableEntity.getTableOwner());
      stmt.setString(14, tableEntity.getDataPath());
      stmt.setLong(15, tableEntity.getDataSize());
      stmt.setString(16, tableEntity.getPermissions());
      stmt.setLong(17, tableEntity.getRowcount());
      stmt.setString(18, tableEntity.getTransformationType());
      stmt.setString(19, tableEntity.getLastData());
      stmt.setString(20, tableEntity.getTimestampField());
      stmt.setString(21, tableEntity.getTimestampFieldFormat());
      stmt.setLong(22, tableEntity.getLastChange());
      stmt.setLong(23, tableEntity.getLastPartitionCreated());
      stmt.setLong(24, tableEntity.getLastSchemaChange());
      stmt.setLong(25, tableEntity.getLastTransformation());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save table", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  public void savePartial(Connection connection, TableEntity tableEntity) {
    String insertTableSql = "insert into table_entity (table_fqdn, table_name, database_name, url_path_prefix, external_table, "
        + "table_description, storage_format, materialize_once, transformation_type, status) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(insertTableSql);
      stmt.setString(1, tableEntity.getFqdn());
      stmt.setString(2, tableEntity.getTableName());
      stmt.setString(3, tableEntity.getDatabaseName());
      stmt.setString(4, tableEntity.getUrlPathPrefix());
      stmt.setBoolean(5, tableEntity.isExternalTable());
      stmt.setString(6, tableEntity.getTableDescription());
      stmt.setString(7, tableEntity.getStorageFormat());
      stmt.setBoolean(8, tableEntity.isMaterializeOnce());
      stmt.setString(9, tableEntity.getTransformationType());
      stmt.setString(10, tableEntity.getStatus());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save table", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  @Override
  public void update(Connection connection, TableEntity tableEntity) {
    String updateTableSql = "update table_entity set " + JDBCUtil.getSetExpressionForClass(TableEntity.class)
        + " where " + TableEntity.TABLE_FQDN + " = ?";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(updateTableSql);
      stmt.setString(1, tableEntity.getFqdn());
      stmt.setString(2, tableEntity.getTableName());
      stmt.setString(3, tableEntity.getDatabaseName());
      stmt.setString(4, tableEntity.getUrlPathPrefix());
      stmt.setBoolean(5, tableEntity.isExternalTable());
      stmt.setString(6, tableEntity.getTableDescription());
      stmt.setString(7, tableEntity.getStorageFormat());
      stmt.setString(8, tableEntity.getInputFormat());
      stmt.setString(9, tableEntity.getOutputFormat());
      stmt.setBoolean(10, tableEntity.isMaterializeOnce());
      stmt.setLong(11, tableEntity.getCreatedAt());
      stmt.setString(12, tableEntity.getStatus());
      stmt.setString(13, tableEntity.getTableOwner());
      stmt.setString(14, tableEntity.getDataPath());
      stmt.setLong(15, tableEntity.getDataSize());
      stmt.setString(16, tableEntity.getPermissions());
      stmt.setLong(17, tableEntity.getRowcount());
      stmt.setString(18, tableEntity.getTransformationType());
      stmt.setString(19, tableEntity.getLastData());
      stmt.setString(20, tableEntity.getTimestampField());
      stmt.setString(21, tableEntity.getTimestampFieldFormat());
      stmt.setLong(22, tableEntity.getLastChange());
      stmt.setLong(23, tableEntity.getLastPartitionCreated());
      stmt.setLong(24, tableEntity.getLastSchemaChange());
      stmt.setLong(25, tableEntity.getLastTransformation());
      stmt.setString(26, tableEntity.getFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not update table", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  public void updatePartial(Connection connection, TableEntity tableEntity) {
    String insertTableSql = "update table_entity set table_fqdn=?, table_name=?, database_name=?, url_path_prefix=?, "
        + "external_table=?, table_description=?, storage_format=?, materialize_once=?, transformation_type=?, status=?"
        + "where " + TableEntity.TABLE_FQDN + " = ?";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(insertTableSql);
      stmt.setString(1, tableEntity.getFqdn());
      stmt.setString(2, tableEntity.getTableName());
      stmt.setString(3, tableEntity.getDatabaseName());
      stmt.setString(4, tableEntity.getUrlPathPrefix());
      stmt.setBoolean(5, tableEntity.isExternalTable());
      stmt.setString(6, tableEntity.getTableDescription());
      stmt.setString(7, tableEntity.getStorageFormat());
      stmt.setBoolean(8, tableEntity.isMaterializeOnce());
      stmt.setString(9, tableEntity.getTransformationType());
      stmt.setString(10, tableEntity.getStatus());
      stmt.setString(11, tableEntity.getFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save table", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  public void updateStatus(Connection connection, TableEntity tableEntity, Long end) {
    String updateTableStatusSql = "update table_entity set " + TableEntity.STATUS + " = ?";
    if (end != null) {
      updateTableStatusSql += ", " + TableEntity.LAST_TRANSFORMATION_TIMESTAMP + " = ?";
    }
    updateTableStatusSql += " where " + TableEntity.TABLE_FQDN + " = ?";

    PreparedStatement stmt = null;
    int i = 1;
    try {
      stmt = connection.prepareStatement(updateTableStatusSql);
      stmt.setString(i++, tableEntity.getStatus());
      if (end != null) {
        stmt.setLong(i++, end);
      }
      stmt.setString(i, tableEntity.getFqdn());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not update table", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

}
