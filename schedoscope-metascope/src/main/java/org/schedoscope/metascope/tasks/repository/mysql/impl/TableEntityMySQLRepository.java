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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.util.JDBCUtil;
import org.schedoscope.metascope.util.MySQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableEntityMySQLRepository implements MySQLRepository<TableEntity> {

  private static final Logger LOG = LoggerFactory.getLogger(TableEntityMySQLRepository.class);

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
  public void insertOrUpdate(Connection connection, TableEntity tableEntity) {
    String insertTableSql = "insert into table_entity (" + JDBCUtil.getDatabaseColumnsForClass(TableEntity.class)
        + ") values (" + JDBCUtil.getValuesCountForClass(TableEntity.class) + ") " + "on duplicate key update "
        + MySQLUtil.getOnDuplicateKeyString(TableEntity.class);
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
      stmt.setLong(16, tableEntity.getRowcount());
      stmt.setString(17, tableEntity.getTransformationType());
      stmt.setString(18, tableEntity.getLastData());
      stmt.setString(19, tableEntity.getTimestampField());
      stmt.setString(20, tableEntity.getTimestampFieldFormat());
      stmt.setLong(21, tableEntity.getLastChange());
      stmt.setLong(22, tableEntity.getLastPartitionCreated());
      stmt.setLong(23, tableEntity.getLastSchemaChange());
      stmt.setLong(24, tableEntity.getLastTransformation());
      stmt.execute();
    } catch (SQLException e) {
      LOG.error("Could not save table", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

  public void insertOrUpdatePartial(Connection connection, TableEntity tableEntity) {
    String insertTableSql = "insert into table_entity (table_fqdn, table_name, database_name, url_path_prefix, external_table, "
        + "table_description, storage_format, materialize_once, transformation_type, status) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "on duplicate key update table_fqdn=values(table_fqdn), table_name=values(table_name), database_name=values(database_name), "
        + "url_path_prefix=values(url_path_prefix),external_table=values(external_table), table_description=values(table_description), "
        + "storage_format=values(storage_format), materialize_once=values(materialize_once), transformation_type=values(transformation_type), "
        + "status=values(status)";
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

  public void insertOrUpdatePartial(Connection connection, List<TableEntity> tables) {
    String insertTableSql = "insert into table_entity (table_fqdn, table_name, database_name, url_path_prefix, external_table, "
        + "table_description, storage_format, materialize_once, transformation_type, status) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "on duplicate key update table_fqdn=values(table_fqdn), table_name=values(table_name), database_name=values(database_name), "
        + "url_path_prefix=values(url_path_prefix),external_table=values(external_table), table_description=values(table_description), "
        + "storage_format=values(storage_format), materialize_once=values(materialize_once), transformation_type=values(transformation_type), "
        + "status=values(status)";
    PreparedStatement stmt = null;
    try {
      int batch = 0;
      connection.setAutoCommit(false);
      stmt = connection.prepareStatement(insertTableSql);
      for (TableEntity tableEntity : tables) {
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

  public void updateStatus(Connection connection, Set<TableEntity> tables) {
    String updateStatus = "update table_entity set status=?, last_transformation_timestamp=? where table_fqdn = ?";
    PreparedStatement updateStatusStmt = null;
    try {
      int updateStatusBatch = 0;
      connection.setAutoCommit(false);
      updateStatusStmt = connection.prepareStatement(updateStatus);
      for (TableEntity tableEntity : tables) {
        updateStatusStmt.setString(1, tableEntity.getStatus());
        updateStatusStmt.setLong(2, tableEntity.getLastTransformation());
        updateStatusStmt.setString(3, tableEntity.getFqdn());
        updateStatusStmt.addBatch();
        updateStatusBatch++;
        if (updateStatusBatch % 1000 == 0) {
          updateStatusStmt.executeBatch();
        }
      }
      updateStatusStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      LOG.error("Could not update table", e);
    } finally {
      DbUtils.closeQuietly(updateStatusStmt);
    }
  }

	@Override
  public void insertOrUpdate(Connection connection, List<TableEntity> entities) {
	  throw new UnsupportedOperationException("Not implemented");
  }

}
