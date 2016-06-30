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
package org.schedoscope.metascope.tasks;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.ExportEntity;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.ParameterValueEntity;
import org.schedoscope.metascope.model.SuccessorEntity;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.TransformationEntity;
import org.schedoscope.metascope.model.ViewDependencyEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.schedoscope.model.View;
import org.schedoscope.metascope.schedoscope.model.ViewField;
import org.schedoscope.metascope.schedoscope.model.ViewStatus;
import org.schedoscope.metascope.schedoscope.model.ViewTransformation;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeConnectException;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class SchedoscopeSyncTask extends Task {

	private static final Logger LOG = LoggerFactory
			.getLogger(SchedoscopeSyncTask.class);

	private static final String OCCURRED_AT = "occurred_at";
	private static final String OCCURRED_UNTIL = "occurred_until";
	private static final String SCHEDOSCOPE_TIMESTAMP_FORMAT = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''";

	public SchedoscopeSyncTask(RepositoryDAO repo, DataSource dataSource,
			SolrFacade solr, MetascopeConfig config,
			SchedoscopeUtil schedoscopeUtil) {
		super(repo, dataSource, solr, config, schedoscopeUtil);
	}

	public boolean run(long start) {
		LOG.info("Retrieve and parse data from schedoscope");

		/* get data from schedoscope */
		ViewStatus viewStatus;
    try {
      viewStatus = schedoscopeUtil.getViewStatus(true);
    } catch (SchedoscopeConnectException e) {
      LOG.error("Could not retrieve view information", e);
      return false;
    }

		if (viewStatus == null || viewStatus.getViews().size() == 0) {
			LOG.info("[SchedoscopeSyncTask] FAILED: Schedoscope status information is not available");
			return false;
		}

		int size = viewStatus.getViews().size();
		LOG.info("Received " + size + " views");

		List<TableEntity> tables = new ArrayList<TableEntity>();
		List<FieldEntity> fields = new ArrayList<FieldEntity>();
		List<TransformationEntity> tes = new ArrayList<TransformationEntity>();
		Map<String, List<ViewEntity>> views = new HashMap<String, List<ViewEntity>>();
		List<ExportEntity> exports = new ArrayList<ExportEntity>();
		List<ParameterValueEntity> parameterValues = new ArrayList<ParameterValueEntity>();
		List<ViewDependencyEntity> dependencies = new ArrayList<ViewDependencyEntity>();
		List<SuccessorEntity> successors = new ArrayList<SuccessorEntity>();
		List<TableDependencyEntity> tableDependencies = new ArrayList<TableDependencyEntity>();
		List<String> customSql = new ArrayList<String>();
		for (View view : viewStatus.getViews()) {
			if (view.isTable()) {
				tables.add(crudTable(view, start, fields, tes, exports,
						customSql));
			} else {
				TableEntity tableEntity = getTable(view.getFqdn(), tables);
				ViewEntity viewEntity = crudView(view, tableEntity,
						dependencies, successors, tableDependencies, start,
						parameterValues);
				List<ViewEntity> list = views.get(tableEntity.getFqdn());
				if (list == null) {
					list = new ArrayList<ViewEntity>();
				}
				list.add(viewEntity);
				views.put(tableEntity.getFqdn(), list);
			}
		}

		for (TableEntity tableEntity : tables) {
			tableEntity.setStatus(schedoscopeUtil.getStatus(views
					.get(tableEntity.getFqdn())));
		}

		LOG.info("Merge data into repository");

		LOG.info("Updating tables ...");
		Connection connection = createConnection();
		repo.insertOrUpdateTablesPartial(connection, tables);
		closeConnection(connection);

		LOG.info("Updating fields ...");
		connection = createConnection();
		repo.insertOrUpdateFields(connection, fields);
		closeConnection(connection);

		LOG.info("Updating transformations ...");
		connection = createConnection();
		repo.insertOrUpdateTransformations(connection, tes);
		closeConnection(connection);

		LOG.info("Updating views ...");
		connection = createConnection();
		repo.insertOrUpdateViewsPartial(connection,
				Lists.newArrayList(Iterables.concat(views.values())));
		closeConnection(connection);

		LOG.info("Updating view values ...");
		connection = createConnection();
		repo.insertOrUpdateParameterValues(connection, parameterValues);
		closeConnection(connection);

		LOG.info("Updating view dependencies ...");
		connection = createConnection();
		repo.insertOrUpdateViewDependencies(connection, dependencies);
		closeConnection(connection);

		LOG.info("Updating view succesors ...");
		connection = createConnection();
		repo.insertOrUpdateSuccessors(connection, successors);
		closeConnection(connection);

		LOG.info("Updating table dependencies ...");
		connection = createConnection();
		repo.insertOrUpdateTableDependencies(connection, tableDependencies);
		closeConnection(connection);

		LOG.info("Updating exports ...");
		connection = createConnection();
		/* drop exports */
		repo.execute(connection, "delete from export_entity_properties");
		repo.execute(connection, "delete from export_entity");
		repo.insertOrUpdateExportPartial(connection, exports);
		closeConnection(connection);

		LOG.info("Updating solr views index");
		for (List<ViewEntity> viewEntityList : views.values()) {
			for (ViewEntity viewEntity : viewEntityList) {
				solr.updateViewEntity(viewEntity, false);
			}
		}

		connection = createConnection();
		for (String sql : customSql) {
			repo.execute(connection, sql);
		}
		DbUtils.closeQuietly(connection);

		LOG.info("Updating solr tables index");
		for (TableEntity tableEntity : tables) {
			solr.updateTablePartial(tableEntity, false);
		}

		try {
			connection.close();
		} catch (SQLException e) {
			LOG.error("Could not close connection to repository", e);
		}

		LOG.info("Commit data to index");
		solr.commit();

		LOG.info("Finished Schedoscope sync task");
		return true;
	}

	private Connection createConnection() {
		Connection connection = null;
		Statement stmt = null;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			LOG.error("Could not connect to repository", e);
		} finally {
			DbUtils.closeQuietly(stmt);
		}
		return connection;
	}

	private void closeConnection(Connection connection) {
		DbUtils.closeQuietly(connection);
	}

	private TableEntity crudTable(View view, long start,
			List<FieldEntity> fields, List<TransformationEntity> tes,
			List<ExportEntity> exports, List<String> customSql) {
		boolean newTable = false;
		TableEntity tableEntity = new TableEntity();
		tableEntity.setFqdn(view.getFqdn());
		tableEntity.setTableName(view.getTableName());
		tableEntity.setDatabaseName(view.getDatabase());
		tableEntity.setUrlPathPrefix(view.getName().endsWith("/") ? view
				.getName() : view.getName() + "/");
		tableEntity.setExternalTable(view.isExternal());
		tableEntity.setTableDescription(view.getComment());
		tableEntity.setStorageFormat(view.getStorageFormat());
		tableEntity.setMaterializeOnce(view.isMaterializeOnce());
		tableEntity.setTransformationType(view.getTransformation().getName());
		boolean setTimestamp = false;
		for (ViewField field : view.getFields()) {
			if (field.getName().equals(OCCURRED_AT)) {
				tableEntity.setTimestampField(OCCURRED_AT);
				tableEntity
						.setTimestampFieldFormat(SCHEDOSCOPE_TIMESTAMP_FORMAT);
				setTimestamp = true;
				break;
			} else if (field.getName().equals(OCCURRED_UNTIL)) {
				tableEntity.setTimestampField(OCCURRED_UNTIL);
				tableEntity
						.setTimestampFieldFormat(SCHEDOSCOPE_TIMESTAMP_FORMAT);
				setTimestamp = true;
				break;
			}
		}

		crudExports(view, tableEntity, exports);
		crudParameters(view, tableEntity, fields);
		crudTransformationProperties(view, tes);
		boolean schemaChanged = crudFields(view, tableEntity, fields);

		if (!newTable && schemaChanged) {
			tableEntity.setLastSchemaChange(start);
			tableEntity.setLastChange(start);
		}

		if (setTimestamp) {
			customSql.add("update table_entity set timestamp_field='"
					+ tableEntity.getTimestampField() + "', "
					+ "timestamp_field_format='"
					+ tableEntity.getTimestampFieldFormat()
					+ "' where table_fqdn='" + tableEntity.getFqdn() + "'");
		}

		return tableEntity;
	}

	private void crudExports(View view, TableEntity tableEntity,
			List<ExportEntity> exports) {
		int i = 0;
		if (view.getExport() != null) {
			for (ViewTransformation export : view.getExport()) {
				ExportEntity exportEntity = new ExportEntity();
				exportEntity.setExportId(tableEntity.getFqdn() + "_" + (i++));
				exportEntity.setFqdn(tableEntity.getFqdn());
				exportEntity.setType(export.getName());
				exportEntity.setProperties(export.getProperties());
				exports.add(exportEntity);
				tableEntity.addToExports(exportEntity);
			}
		}
	}

	private void crudParameters(View view, TableEntity tableEntity,
			List<FieldEntity> fields) {
		int i = 0;
		for (ViewField viewField : view.getParameters()) {
			FieldEntity parameter = new FieldEntity();
			parameter.setFqdn(view.getFqdn());
			parameter.setName(viewField.getName());
			parameter.setType(viewField.getFieldtype());
			parameter.setParameterField(true);
			parameter.setFieldOrder(i++);
			parameter.setDescription(viewField.getComment());
			tableEntity.addToFields(parameter);
			fields.add(parameter);
		}
	}

	private void crudTransformationProperties(View view,
			List<TransformationEntity> tes) {
		for (Entry<String, String> viewTe : view.getTransformation()
				.getProperties().entrySet()) {
			TransformationEntity te = new TransformationEntity();
			te.setFqdn(view.getFqdn());
			te.setTransformationKey(viewTe.getKey());
			te.setTransformationValue(viewTe.getValue());
			tes.add(te);
		}
	}

	private boolean crudFields(View view, TableEntity tableEntity,
			List<FieldEntity> fields) {
		int i = 0;
		for (ViewField viewField : view.getFields()) {
			FieldEntity fieldEntity = new FieldEntity();
			fieldEntity.setFqdn(view.getFqdn());
			fieldEntity.setName(viewField.getName());
			fieldEntity.setType(viewField.getFieldtype());
			fieldEntity.setParameterField(false);
			fieldEntity.setFieldOrder(i++);
			fieldEntity.setDescription(viewField.getComment());
			tableEntity.addToFields(fieldEntity);
			fields.add(fieldEntity);
		}
		return false;
	}

	private ViewEntity crudView(View view, TableEntity tableEntity,
			List<ViewDependencyEntity> dependencies,
			List<SuccessorEntity> successors,
			List<TableDependencyEntity> tableDependencies, long start,
			List<ParameterValueEntity> parameterValues) {
		ViewEntity viewEntity = new ViewEntity();
		viewEntity.setUrlPath(view.getName());
		viewEntity.setFqdn(view.getFqdn());
		viewEntity.setStatus(view.getStatus());
		viewEntity.setParameterString(getParameterValues(view.getName(),
				tableEntity.getParameters()));
		viewEntity.setInternalViewId(view.getViewId());
		viewEntity.setTable(tableEntity);

		crudParameterValues(view, tableEntity, viewEntity, parameterValues);
		crudDependencies(view, dependencies, successors, tableDependencies);

		return viewEntity;
	}

	private void crudParameterValues(View view, TableEntity tableEntity,
			ViewEntity viewEntity, List<ParameterValueEntity> parameterValues) {
		Map<String, String> keyValues = getParameterAsMap(view.getName(),
				tableEntity.getParameters());
		for (Entry<String, String> viewParameter : keyValues.entrySet()) {
			ParameterValueEntity parameterValueEntity = new ParameterValueEntity();
			parameterValueEntity.setUrlPath(view.getName());
			parameterValueEntity.setKey(viewParameter.getKey());
			parameterValueEntity.setValue(viewParameter.getValue());
			parameterValueEntity.setTableFqdn(tableEntity.getFqdn());
			viewEntity.addToParameter(parameterValueEntity);
			parameterValues.add(parameterValueEntity);
		}
	}

	private void crudDependencies(View view,
			List<ViewDependencyEntity> dependencies,
			List<SuccessorEntity> successors,
			List<TableDependencyEntity> tableDependencies) {
		for (Entry<String, List<String>> dependencyEntry : view
				.getDependencies().entrySet()) {
			String dependencyFqdn = dependencyEntry.getKey();

			// create view dependencies
			for (String dependencyUrlPath : dependencyEntry.getValue()) {
				ViewDependencyEntity dependency = new ViewDependencyEntity();
				dependency.setUrlPath(view.getName());
				dependency.setDependencyUrlPath(dependencyUrlPath);
				dependency.setDependencyFqdn(dependencyFqdn);
				dependency
						.setInternalViewId(viewIdFromUrlPath(dependencyUrlPath));
				dependencies.add(dependency);

				SuccessorEntity successor = new SuccessorEntity();
				successor.setUrlPath(dependencyUrlPath);
				successor.setSuccessorUrlPath(view.getName());
				successor.setSuccessorFqdn(view.getFqdn());
				successor.setInternalViewId(viewIdFromUrlPath(view.getName()));
				successors.add(successor);
			}

			TableDependencyEntity dependency = new TableDependencyEntity();
			dependency.setFqdn(view.getFqdn());
			dependency.setDependencyFqdn(dependencyFqdn);
			tableDependencies.add(dependency);
		}
	}

	/* HELPER METHODS */

	private TableEntity getTable(String fqdn, List<TableEntity> tables) {
		for (TableEntity tableEntity : tables) {
			if (tableEntity.getFqdn().equals(fqdn)) {
				return tableEntity;
			}
		}
		return null;
	}

	private String getParameterValues(String urlPath,
			List<FieldEntity> parameters) {
		int index = StringUtils.ordinalIndexOf(urlPath, "/", 2) + 1;
		String[] parameterValues = urlPath.substring(index).split("/");
		String result = "";
		if (parameters != null) {
			for (int i = 0; i < parameters.size(); i++) {
				FieldEntity parameter = parameters.get(i);
				if (!result.isEmpty()) {
					result += "/";
				}
				result += parameter.getName() + "=" + parameterValues[i];
			}
		}
		return result;
	}

	private Map<String, String> getParameterAsMap(String urlPath,
			List<FieldEntity> parameters) {
		int index = StringUtils.ordinalIndexOf(urlPath, "/", 2) + 1;
		String[] parameterValues = urlPath.substring(index).split("/");
		Map<String, String> map = new HashMap<String, String>();
		if (parameters != null) {
			for (int i = 0; i < parameters.size(); i++) {
				FieldEntity parameter = parameters.get(i);
				map.put(parameter.getName(), parameterValues[i]);
			}
		}
		return map;
	}

	private String viewIdFromUrlPath(String dependencyUrlPath) {
		int index = StringUtils.ordinalIndexOf(dependencyUrlPath, "/", 2) + 1;
		String[] parameterValues = dependencyUrlPath.substring(index)
				.split("/");
		String result = "";

		if (parameterValues.length == 1 && parameterValues[0].isEmpty()) {
			return "root";
		}

		for (int i = 0; i < parameterValues.length; i++) {
			result += parameterValues[i];
		}
		return result;
	}

}
