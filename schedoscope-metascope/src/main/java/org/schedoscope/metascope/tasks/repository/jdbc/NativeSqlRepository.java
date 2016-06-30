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
package org.schedoscope.metascope.tasks.repository.jdbc;

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
import org.schedoscope.metascope.tasks.repository.jdbc.impl.ExportEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.FieldEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.MetadataJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.ParameterValueEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.SuccessorEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.TableDependencyEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.TableEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.TransformationEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.ViewDependencyEntityJdbcRepository;
import org.schedoscope.metascope.tasks.repository.jdbc.impl.ViewEntityJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeSqlRepository implements RepositoryDAO {

  private static final Logger LOG = LoggerFactory.getLogger(NativeSqlRepository.class);

  private TableEntityJdbcRepository tableEntityJdbcRepository;
  private ViewEntityJdbcRepository viewEntityJdbcRepository;
  private FieldEntityJdbcRepository fieldEntityJdbcRepository;
  private SuccessorEntityJdbcRepository successorEntityJdbcRepository;
  private TransformationEntityJdbcRepository transformationEntityJdbcRepository;
  private ViewDependencyEntityJdbcRepository viewDependencyEntityJdbcRepository;
  private ParameterValueEntityJdbcRepository parameterValueEntityJdbcRepository;
  private TableDependencyEntityJdbcRepository tableDependencyEntityJdbcRepository;
  private MetadataJdbcRepository metadataJdbcRepository;
  private ExportEntityJdbcRepository exportEntityJdbcRepository;

  public NativeSqlRepository() {
    this.tableEntityJdbcRepository = new TableEntityJdbcRepository();
    this.viewEntityJdbcRepository = new ViewEntityJdbcRepository();
    this.fieldEntityJdbcRepository = new FieldEntityJdbcRepository();
    this.successorEntityJdbcRepository = new SuccessorEntityJdbcRepository();
    this.transformationEntityJdbcRepository = new TransformationEntityJdbcRepository();
    this.viewDependencyEntityJdbcRepository = new ViewDependencyEntityJdbcRepository();
    this.parameterValueEntityJdbcRepository = new ParameterValueEntityJdbcRepository();
    this.tableDependencyEntityJdbcRepository = new TableDependencyEntityJdbcRepository();
    this.metadataJdbcRepository = new MetadataJdbcRepository();
    this.exportEntityJdbcRepository = new ExportEntityJdbcRepository();
  }

  @Override
  public List<TableEntity> getTables(Connection connection) {
    return tableEntityJdbcRepository.get(connection);
  }

  @Override
  public List<ViewEntity> getViews(Connection connection) {
    return viewEntityJdbcRepository.get(connection);
  }

  @Override
  public Metadata getMetadata(Connection connection, String key) {
    return metadataJdbcRepository.get(connection, key);
  }

  @Override
  public void updateTableStatus(Connection connection, TableEntity tableEntity, Long end) {
    tableEntityJdbcRepository.updateStatus(connection, tableEntity, end);
  }

  @Override
  public void updateViewStatus(Connection connection, ViewEntity viewEntity, Long start, Long end) {
    viewEntityJdbcRepository.updateStatus(connection, viewEntity, start, end);
  }

  @Override
  public void insertOrUpdatePartial(Connection connection, TableEntity tableEntity) {
    TableEntity t = tableEntityJdbcRepository.get(connection, tableEntity.getFqdn());
    if (t == null) {
      tableEntityJdbcRepository.savePartial(connection, tableEntity);
    } else {
      tableEntityJdbcRepository.updatePartial(connection, tableEntity);
    }
  }

  @Override
  public void insertOrUpdatePartial(Connection connection, ViewEntity viewEntity) {
    ViewEntity v = viewEntityJdbcRepository.get(connection, viewEntity.getUrlPath());
    if (v == null) {
      viewEntityJdbcRepository.savePartial(connection, viewEntity);
    } else {
      viewEntityJdbcRepository.updatePartial(connection, viewEntity);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, SuccessorEntity successorEntity) {
    SuccessorEntity s = successorEntityJdbcRepository.get(connection, successorEntity.getEntityKey());
    if (s == null) {
      successorEntityJdbcRepository.save(connection, successorEntity);
    } else {
      successorEntityJdbcRepository.update(connection, successorEntity);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, TableEntity tableEntity) {
    TableEntity t = tableEntityJdbcRepository.get(connection, tableEntity.getFqdn());
    if (t == null) {
      tableEntityJdbcRepository.save(connection, tableEntity);
    } else {
      tableEntityJdbcRepository.update(connection, tableEntity);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, FieldEntity fieldEntity) {
    FieldEntity f = fieldEntityJdbcRepository.get(connection, fieldEntity.getEntityKey());
    if (f == null) {
      fieldEntityJdbcRepository.save(connection, fieldEntity);
    } else {
      fieldEntityJdbcRepository.update(connection, fieldEntity);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, TransformationEntity te) {
    TransformationEntity t = transformationEntityJdbcRepository.get(connection, te.getEntityKey());
    if (t == null) {
      transformationEntityJdbcRepository.save(connection, te);
    } else {
      transformationEntityJdbcRepository.update(connection, te);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, ViewEntity viewEntity) {
    ViewEntity v = viewEntityJdbcRepository.get(connection, viewEntity.getUrlPath());
    if (v == null) {
      viewEntityJdbcRepository.save(connection, viewEntity);
    } else {
      viewEntityJdbcRepository.update(connection, viewEntity);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, ParameterValueEntity parameterValueEntity) {
    ParameterValueEntity p = parameterValueEntityJdbcRepository.get(connection, parameterValueEntity.getEntityKey());
    if (p == null) {
      parameterValueEntityJdbcRepository.save(connection, parameterValueEntity);
    } else {
      parameterValueEntityJdbcRepository.update(connection, parameterValueEntity);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, ViewDependencyEntity dependency) {
    ViewDependencyEntity v = viewDependencyEntityJdbcRepository.get(connection, dependency.getEntityKey());
    if (v == null) {
      viewDependencyEntityJdbcRepository.save(connection, dependency);
    } else {
      viewDependencyEntityJdbcRepository.update(connection, dependency);
    }
  }

  @Override
  public void insertOrUpdate(Connection connection, TableDependencyEntity dependency) {
    TableDependencyEntity t = tableDependencyEntityJdbcRepository.get(connection, dependency.getEntityKey());
    if (t == null) {
      tableDependencyEntityJdbcRepository.save(connection, dependency);
    } else {
      tableDependencyEntityJdbcRepository.update(connection, dependency);
    }
  }

  private void insertOrUpdate(Connection connection, ExportEntity exportEntity) {
    exportEntityJdbcRepository.save(connection, exportEntity);
  }

  @Override
  public void insertOrUpdate(Connection connection, Metadata metadata) {
    Metadata m = metadataJdbcRepository.get(connection, metadata.getMetadataKey());
    if (m == null) {
      metadataJdbcRepository.save(connection, metadata);
    } else {
      metadataJdbcRepository.update(connection, metadata);
    }
  }

  @Override
  public void insertOrUpdateTablesPartial(Connection connection, List<TableEntity> tables) {
    for (TableEntity tableEntity : tables) {
      insertOrUpdatePartial(connection, tableEntity);
    }
  }

  @Override
  public void insertOrUpdateViewsPartial(Connection connection, List<ViewEntity> views) {
    for (ViewEntity viewEntity : views) {
      insertOrUpdatePartial(connection, viewEntity);
    }
  }

  @Override
  public void insertOrUpdateFields(Connection connection, List<FieldEntity> fields) {
    for (FieldEntity fieldEntity : fields) {
      insertOrUpdate(connection, fieldEntity);
    }
  }

  @Override
  public void insertOrUpdateTransformations(Connection connection, List<TransformationEntity> tes) {
    for (TransformationEntity te : tes) {
      insertOrUpdate(connection, te);
    }
  }

  @Override
  public void insertOrUpdateParameterValues(Connection connection, List<ParameterValueEntity> parameterValues) {
    for (ParameterValueEntity parameterValueEntity : parameterValues) {
      insertOrUpdate(connection, parameterValueEntity);
    }
  }

  @Override
  public void insertOrUpdateViewDependencies(Connection connection, List<ViewDependencyEntity> dependencies) {
    for (ViewDependencyEntity viewDependencyEntity : dependencies) {
      insertOrUpdate(connection, viewDependencyEntity);
    }
  }

  @Override
  public void insertOrUpdateSuccessors(Connection connection, List<SuccessorEntity> successors) {
    for (SuccessorEntity successorEntity : successors) {
      insertOrUpdate(connection, successorEntity);
    }
  }

  @Override
  public void insertOrUpdateTableDependencies(Connection connection, List<TableDependencyEntity> tableDependencies) {
    for (TableDependencyEntity tableDependencyEntity : tableDependencies) {
      insertOrUpdate(connection, tableDependencyEntity);
    }
  }

  @Override
  public void insertOrUpdateExportPartial(Connection connection, List<ExportEntity> exports) {
    for (ExportEntity exportEntity : exports) {
      insertOrUpdate(connection, exportEntity);
    }
  }

  @Override
  public void updateTableStatus(Connection connection, Set<TableEntity> tables) {
    for (TableEntity tableEntity : tables) {
      updateTableStatus(connection, tableEntity, tableEntity.getLastTransformation());
    }
  }

  @Override
  public void updateViewStatus(Connection connection, Set<ViewEntity> views) {
    for (ViewEntity viewEntity : views) {
      updateViewStatus(connection, viewEntity, viewEntity.getTransformationStart(), viewEntity.getTransformationEnd());
    }
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