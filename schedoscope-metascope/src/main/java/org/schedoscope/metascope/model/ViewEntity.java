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
package org.schedoscope.metascope.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

@Entity
public class ViewEntity {

	public static final String URL_PATH = "url_path";
	public static final String FQDN = "fqdn";
	public static final String STATUS = "status";
	public static final String PARAMETERSTRING = "parameter_string";
	public static final String INTERNAL_VIEW_ID = "internal_view_id";
	public static final String TRANSFORMATION_START = "transformation_start";
	public static final String TRANSFORMATION_END = "transformation_end";
	public static final String CREATED_AT = "created_at";
	public static final String ROW_JOB_FINISHED = "row_job_finished";
	public static final String ROWCOUNT = "rowcount";
	public static final String TABLE_FQDN = "table_fqdn";

	@Id
	@Column(name = URL_PATH)
	private String urlPath;
	@Column(name = FQDN)
	private String fqdn;
	@Column(name = STATUS)
	private String status;
	@Column(name = PARAMETERSTRING)
	private String parameterString;
	@Column(name = INTERNAL_VIEW_ID)
	private String internalViewId;
	@Column(name = TRANSFORMATION_START, columnDefinition = "bigint default 0")
	private long transformationStart;
	@Column(name = TRANSFORMATION_END, columnDefinition = "bigint default 0")
	private long transformationEnd;
	@Column(name = CREATED_AT, columnDefinition = "bigint default 0")
	private long createdAt;
	@Column(name = ROW_JOB_FINISHED, columnDefinition = "bigint default 0")
	private boolean rowJobFinished;
	@Column(name = ROWCOUNT, columnDefinition = "bigint default 0")
	private long rows;

	@ManyToOne(optional = false, fetch = FetchType.LAZY)
	@JoinColumn(name = TABLE_FQDN, nullable = false)
	private TableEntity table;

	@OneToMany(mappedBy = "key.urlPath", fetch = FetchType.LAZY)
	private List<JobMetadataEntity> jobProcessMetadata;

	@OneToMany(mappedBy = "key.urlPath", fetch = FetchType.EAGER)
	private List<ParameterValueEntity> parameters;

	@OneToMany(mappedBy = "key.urlPath", fetch = FetchType.LAZY)
	private List<ViewDependencyEntity> dependencies;

	@OneToMany(mappedBy = "key.urlPath", fetch = FetchType.LAZY)
	private List<SuccessorEntity> successors;

	@OneToMany(mappedBy = "key.urlPath", fetch = FetchType.LAZY)
	private List<DataDistributionEntity> dataDistribution;

	public String getUrlPath() {
		return urlPath;
	}

	public void setUrlPath(String urlPath) {
		this.urlPath = urlPath;
	}

	public TableEntity getTable() {
		return table;
	}

	public void setTable(TableEntity table) {
		this.table = table;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getFqdn() {
		return fqdn;
	}

	public void setFqdn(String fqdn) {
		this.fqdn = fqdn;
	}

	public List<ParameterValueEntity> getParameters() {
		return parameters;
	}

	public void setParameters(List<ParameterValueEntity> parameters) {
		this.parameters = parameters;
	}

	public String getParameterString() {
		return parameterString;
	}

	public void setParameterString(String parameterString) {
		this.parameterString = parameterString;
	}

	public long getTransformationStart() {
		return transformationStart;
	}

	public void setTransformationStart(long transformationStart) {
		this.transformationStart = transformationStart;
	}

	public long getTransformationEnd() {
		return transformationEnd;
	}

	public void setTransformationEnd(long transformationEnd) {
		this.transformationEnd = transformationEnd;
	}

	public long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(long createdAt) {
		this.createdAt = createdAt;
	}

	public boolean isProcessing() {
		for (JobMetadataEntity jobMetadataEntity : jobProcessMetadata) {
			if (!jobMetadataEntity.isFinished()) {
				return true;
			}
		}
		return false;
	}

	public boolean isProcessed() {
		if (!rowJobFinished) {
			return false;
		}

		if (isProcessing()) {
			return false;
		}

		return true;
	}

	public boolean isRowJobFinished() {
		return rowJobFinished;
	}

	public void setRowJobFinished(boolean rowJobFinished) {
		this.rowJobFinished = rowJobFinished;
	}

	public String getInternalViewId() {
		return internalViewId;
	}

	public void setInternalViewId(String internalViewId) {
		this.internalViewId = internalViewId;
	}

	public long getRows() {
		return rows;
	}

	public void setRows(long rows) {
		this.rows = rows;
	}

	public List<ViewDependencyEntity> getDependencies() {
		return dependencies;
	}

	public void setDependencies(List<ViewDependencyEntity> dependencies) {
		this.dependencies = dependencies;
	}

	public Map<String, List<ViewDependencyEntity>> getDependencyMap() {
		Map<String, List<ViewDependencyEntity>> map = new HashMap<String, List<ViewDependencyEntity>>();
		for (ViewDependencyEntity dependencyEntity : dependencies) {
			List<ViewDependencyEntity> list = map.get(dependencyEntity
					.getDependencyFqdn());
			if (list == null) {
				list = new ArrayList<ViewDependencyEntity>();
			}
			list.add(dependencyEntity);
			map.put(dependencyEntity.getDependencyFqdn(), list);
		}
		return map;
	}

	public List<SuccessorEntity> getSuccessors() {
		return successors;
	}

	public void setSuccessors(List<SuccessorEntity> successors) {
		this.successors = successors;
	}

	public Map<String, List<SuccessorEntity>> getSuccessorMap() {
		Map<String, List<SuccessorEntity>> map = new HashMap<String, List<SuccessorEntity>>();
		for (SuccessorEntity successorEntity : successors) {
			List<SuccessorEntity> list = map.get(successorEntity
					.getSuccessorFqdn());
			if (list == null) {
				list = new ArrayList<SuccessorEntity>();
			}
			list.add(successorEntity);
			map.put(successorEntity.getSuccessorFqdn(), list);
		}
		return map;
	}

	public List<DataDistributionEntity> getDataDistribution() {
		return dataDistribution;
	}

	public void setDataDistribution(
			List<DataDistributionEntity> dataDistributionEntity) {
		this.dataDistribution = dataDistributionEntity;
	}

	public String getDataDistributionForFieldAndKey(String fieldName, String key) {
		for (DataDistributionEntity dde : dataDistribution) {
			if (dde.getFieldName().equals(fieldName)
					&& dde.getDistributionKey().equals(key)) {
				return dde.getDistributionValue();
			}
		}
		return null;
	}

	public String getValueForParameterName(String name) {
		for (ParameterValueEntity parameterValueEntity : parameters) {
			if (parameterValueEntity.getKey().equals(name)) {
				return parameterValueEntity.getValue();
			}
		}
		return null;
	}

	public void addToParameter(ParameterValueEntity parameterValueEntity) {
		if (this.parameters == null) {
			this.parameters = new ArrayList<ParameterValueEntity>();
		}
		this.parameters.add(parameterValueEntity);
	}

}
