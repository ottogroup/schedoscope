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
package org.schedoscope.metascope.tasks.repository.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbutils.DbUtils;
import org.schedoscope.metascope.model.ExportEntity;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.Metadata;
import org.schedoscope.metascope.model.ParameterValueEntity;
import org.schedoscope.metascope.model.SuccessorEntity;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.TransformationEntity;
import org.schedoscope.metascope.model.ViewDependencyEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.tasks.repository.mysql.impl.ExportEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.FieldEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.MetadataMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.ParameterValueEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.SuccessorEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.TableDependencyEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.TableEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.TransformationEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.ViewDependencyEntityMySQLRepository;
import org.schedoscope.metascope.tasks.repository.mysql.impl.ViewEntityMySQLRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLRepository implements RepositoryDAO {

  private static final Logger LOG = LoggerFactory.getLogger(FieldEntityMySQLRepository.class);

  private TableEntityMySQLRepository tableEntityMySQLRepository;
  private ViewEntityMySQLRepository viewEntityMySQLRepository;
  private FieldEntityMySQLRepository fieldEntityMySQLRepository;
  private MetadataMySQLRepository metadataMySQLRepository;
  private ParameterValueEntityMySQLRepository parameterValueEntityMySQLRepository;
  private SuccessorEntityMySQLRepository successorEntityMySQLRepository;
  private TableDependencyEntityMySQLRepository tableDependencyEntityMySQLRepository;
  private TransformationEntityMySQLRepository transformationEntityMySQLRepository;
  private ViewDependencyEntityMySQLRepository viewDependencyEntityMySQLRepository;
  private ExportEntityMySQLRepository exportEntityMySQLRepository;

  public MySQLRepository() {
    this.tableEntityMySQLRepository = new TableEntityMySQLRepository();
    this.viewEntityMySQLRepository = new ViewEntityMySQLRepository();
    this.fieldEntityMySQLRepository = new FieldEntityMySQLRepository();
    this.metadataMySQLRepository = new MetadataMySQLRepository();
    this.parameterValueEntityMySQLRepository = new ParameterValueEntityMySQLRepository();
    this.successorEntityMySQLRepository = new SuccessorEntityMySQLRepository();
    this.tableDependencyEntityMySQLRepository = new TableDependencyEntityMySQLRepository();
    this.transformationEntityMySQLRepository = new TransformationEntityMySQLRepository();
    this.viewDependencyEntityMySQLRepository = new ViewDependencyEntityMySQLRepository();
    this.exportEntityMySQLRepository = new ExportEntityMySQLRepository();
  }

  @Override
  public List<TableEntity> getTables(Connection connection) {
    return tableEntityMySQLRepository.get(connection);
  }

  @Override
  public List<ViewEntity> getViews(Connection connection) {
    return viewEntityMySQLRepository.get(connection);
  }

  @Override
  public Metadata getMetadata(Connection connection, String key) {
    return metadataMySQLRepository.get(connection, key);
  }

  @Override
  public void insertOrUpdatePartial(Connection connection, TableEntity tableEntity) {
    tableEntityMySQLRepository.insertOrUpdatePartial(connection, tableEntity);
  }

  @Override
  public void insertOrUpdatePartial(Connection connection, ViewEntity viewEntity) {
    viewEntityMySQLRepository.insertOrUpdatePartial(connection, viewEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, SuccessorEntity successorEntity) {
    successorEntityMySQLRepository.insertOrUpdate(connection, successorEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, TableEntity tableEntity) {
    tableEntityMySQLRepository.insertOrUpdate(connection, tableEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, FieldEntity fieldEntity) {
    fieldEntityMySQLRepository.insertOrUpdate(connection, fieldEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, TransformationEntity te) {
    transformationEntityMySQLRepository.insertOrUpdate(connection, te);
  }

  @Override
  public void insertOrUpdate(Connection connection, ViewEntity viewEntity) {
    viewEntityMySQLRepository.insertOrUpdate(connection, viewEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, ParameterValueEntity parameterValueEntity) {
    parameterValueEntityMySQLRepository.insertOrUpdate(connection, parameterValueEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, ViewDependencyEntity dependency) {
    viewDependencyEntityMySQLRepository.insertOrUpdate(connection, dependency);
  }

  @Override
  public void insertOrUpdate(Connection connection, TableDependencyEntity dependency) {
    tableDependencyEntityMySQLRepository.insertOrUpdate(connection, dependency);
  }

  @Override
  public void insertOrUpdate(Connection connection, Metadata metadata) {
    metadataMySQLRepository.insertOrUpdate(connection, metadata);
  }

  @Override
  public void insertOrUpdateTablesPartial(Connection connection, List<TableEntity> tables) {
    tableEntityMySQLRepository.insertOrUpdatePartial(connection, tables);
  }

  @Override
  public void insertOrUpdateViewsPartial(Connection connection, List<ViewEntity> views) {
    viewEntityMySQLRepository.insertOrUpdatePartial(connection, views);
  }

  @Override
  public void insertOrUpdateFields(Connection connection, List<FieldEntity> fields) {
    fieldEntityMySQLRepository.insertOrUpdate(connection, fields);
  }

  @Override
  public void insertOrUpdateTransformations(Connection connection, List<TransformationEntity> tes) {
    transformationEntityMySQLRepository.insertOrUpdate(connection, tes);
  }

  @Override
  public void insertOrUpdateParameterValues(Connection connection, List<ParameterValueEntity> parameterValues) {
    parameterValueEntityMySQLRepository.insertOrUpdate(connection, parameterValues);
  }

  @Override
  public void insertOrUpdateViewDependencies(Connection connection, List<ViewDependencyEntity> dependencies) {
    viewDependencyEntityMySQLRepository.insertOrUpdate(connection, dependencies);
  }

  @Override
  public void insertOrUpdateSuccessors(Connection connection, List<SuccessorEntity> successors) {
    successorEntityMySQLRepository.insertOrUpdate(connection, successors);
  }

  @Override
  public void insertOrUpdateTableDependencies(Connection connection, List<TableDependencyEntity> tableDependencies) {
    tableDependencyEntityMySQLRepository.insertOrUpdate(connection, tableDependencies);
  }

  @Override
  public void insertOrUpdateExportPartial(Connection connection, List<ExportEntity> exports) {
    exportEntityMySQLRepository.insertOrUpdate(connection, exports);
  }

  @Override
  public void updateTableStatus(Connection connection, TableEntity tableEntity, Long lastTransformation) {
    tableEntityMySQLRepository.updateStatus(connection, tableEntity, lastTransformation);
  }

  @Override
  public void updateTableStatus(Connection connection, Set<TableEntity> tables) {
    tableEntityMySQLRepository.updateStatus(connection, tables);
  }

  @Override
  public void updateViewStatus(Connection connection, Set<ViewEntity> views) {
    viewEntityMySQLRepository.updateStatus(connection, views);
  }

  @Override
  public void updateViewStatus(Connection connection, ViewEntity viewEntity, Long start, Long end) {
    viewEntityMySQLRepository.updateStatus(connection, viewEntity, start, end);
  }

  @Override
  public void execute(Connection connection, String sql) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      stmt.execute(sql);
    } catch (SQLException e) {
      LOG.error("Could not execute query '" + sql + "'", e);
    } finally {
      DbUtils.closeQuietly(stmt);
    }
  }

}
