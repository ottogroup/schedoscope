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
package org.schedoscope.metascope.tasks.repository;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

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

public interface RepositoryDAO {

  public List<TableEntity> getTables(Connection connection);

  public List<ViewEntity> getViews(Connection connection);

  public Metadata getMetadata(Connection connection, String string);

  public void insertOrUpdatePartial(Connection connection, TableEntity tableEntity);

  public void insertOrUpdatePartial(Connection connection, ViewEntity ciewEntity);

  public void insertOrUpdate(Connection connection, SuccessorEntity successorEntity);

  public void insertOrUpdate(Connection connection, TableEntity tableEntity);

  public void insertOrUpdate(Connection connection, FieldEntity fieldEntity);

  public void insertOrUpdate(Connection connection, TransformationEntity te);

  public void insertOrUpdate(Connection connection, ViewEntity viewEntity);

  public void insertOrUpdate(Connection connection, ParameterValueEntity parameterValueEntity);

  public void insertOrUpdate(Connection connection, ViewDependencyEntity dependency);

  public void insertOrUpdate(Connection connection, TableDependencyEntity dependency);

  public void insertOrUpdate(Connection connection, Metadata metadata);

  public void insertOrUpdateTablesPartial(Connection connection, List<TableEntity> tables);

  public void insertOrUpdateViewsPartial(Connection connection, List<ViewEntity> concat);

  public void insertOrUpdateFields(Connection connection, List<FieldEntity> fields);

  public void insertOrUpdateTransformations(Connection connection, List<TransformationEntity> tes);

  public void insertOrUpdateParameterValues(Connection connection, List<ParameterValueEntity> parameterValues);

  public void insertOrUpdateViewDependencies(Connection connection, List<ViewDependencyEntity> dependencies);

  public void insertOrUpdateSuccessors(Connection connection, List<SuccessorEntity> successors);

  public void insertOrUpdateTableDependencies(Connection connection, List<TableDependencyEntity> tableDependencies);
  
	public void insertOrUpdateExportPartial(Connection connection, List<ExportEntity> exports);

  public void updateTableStatus(Connection connection, TableEntity tableEntity, Long lastTransformation);

  public void updateViewStatus(Connection connection, ViewEntity viewEntity, Long start, Long end);

  public void updateTableStatus(Connection connection, Set<TableEntity> tables);

  public void updateViewStatus(Connection connection, Set<ViewEntity> views);

  public void execute(Connection connection, String sql);

}
