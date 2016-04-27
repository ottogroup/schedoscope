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
import java.util.List;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;

@Entity
public class TableEntity extends Documentable {

  public static final String TABLE_FQDN = "table_fqdn";
  public static final String TABLE_NAME = "table_name";
  public static final String DATABASE_NAME = "database_name";
  public static final String URL_PATH_PREFIX = "url_path_prefix";
  public static final String EXTERNAL_TABLE = "external_table";
  public static final String TABLE_DESCRIPTION = "table_description";
  public static final String STORAGE_FORMAT = "storage_format";
  public static final String INPUT_FORMAT = "input_format";
  public static final String OUTPUT_FORMAT = "output_format";
  public static final String MATERIALIZE_ONCE = "materialize_once";
  public static final String CREATED_AT = "created_at";
  public static final String STATUS = "status";
  public static final String TABLE_OWNER = "table_owner";
  public static final String DATA_PATH = "data_path";
  public static final String DATA_SIZE = "data_size";
  public static final String ROWCOUNT = "rowcount";
  public static final String TRANSFORMATION_TYPE = "transformation_type";
  public static final String LAST_DATA = "last_data";
  public static final String TIMESTAMP_FIELD = "timestamp_field";
  public static final String TIMESTAMP_FIELD_FORMAT = "timestamp_field_format";
  public static final String LAST_CHANGE = "last_change";
  public static final String LAST_PARTITION_CREATED = "last_partition_created";
  public static final String LAST_SCHEMA_CHANGE = "last_schema_change";
  public static final String LAST_TRANSFORMATION_TIMESTAMP = "last_transformation_timestamp";

  @Id
  @Column(name = TABLE_FQDN)
  private String fqdn;
  @Column(name = TABLE_NAME)
  private String tableName;
  @Column(name = DATABASE_NAME)
  private String databaseName;
  @Column(name = URL_PATH_PREFIX)
  private String urlPathPrefix;
  @Column(name = EXTERNAL_TABLE)
  private boolean externalTable;
  @Column(name = TABLE_DESCRIPTION)
  private String tableDescription;
  @Column(name = STORAGE_FORMAT)
  private String storageFormat;
  @Column(name = INPUT_FORMAT)
  private String inputFormat;
  @Column(name = OUTPUT_FORMAT)
  private String outputFormat;
  @Column(name = MATERIALIZE_ONCE)
  private boolean materializeOnce;
  @Column(name = CREATED_AT, columnDefinition = "bigint default 0")
  private long createdAt;
  @Column(name = STATUS)
  private String status;
  @Column(name = TABLE_OWNER)
  private String tableOwner;
  @Column(name = DATA_PATH)
  private String dataPath;
  @Column(name = DATA_SIZE, columnDefinition = "bigint default 0")
  private long dataSize;
  @Column(name = ROWCOUNT, columnDefinition = "bigint default 0")
  private long rowcount;
  @Column(name = TRANSFORMATION_TYPE)
  private String transformationType;
  @Column(name = LAST_DATA)
  private String lastData;
  @Column(name = TIMESTAMP_FIELD)
  private String timestampField;
  @Column(name = TIMESTAMP_FIELD_FORMAT)
  private String timestampFieldFormat;
  @Column(name = LAST_CHANGE, columnDefinition = "bigint default 0")
  private long lastChange;
  @Column(name = LAST_PARTITION_CREATED, columnDefinition = "bigint default 0")
  private long lastPartitionCreated;
  @Column(name = LAST_SCHEMA_CHANGE, columnDefinition = "bigint default 0")
  private long lastSchemaChange;
  @Column(name = LAST_TRANSFORMATION_TIMESTAMP, columnDefinition = "bigint default 0")
  private long lastTransformationTimestamp;
  @Column(name = "view_count", columnDefinition = "int default 0")
  private int viewCount;

  private String personResponsible;

  @OneToMany(mappedBy = "key.fqdn", fetch = FetchType.EAGER)
  @OrderBy(value = "fieldOrder")
  private List<FieldEntity> fields;

  @OneToMany(mappedBy = "table", fetch = FetchType.LAZY)
  private List<ViewEntity> views;

  @OneToMany(mappedBy = "key.fqdn", fetch = FetchType.LAZY)
  private List<TableDependencyEntity> dependencies;

  @OneToMany(mappedBy = "key.fqdn", fetch = FetchType.LAZY)
  private List<TransformationEntity> transformationProperties;

  @ManyToMany(fetch = FetchType.EAGER)
  private List<BusinessObjectEntity> businessObjects;

  @ElementCollection(fetch = FetchType.EAGER)
  @CollectionTable(name = "table_tags", joinColumns = @JoinColumn(name = "fqdn"))
  private List<String> tags;

  public TableEntity() {
    this.fields = new ArrayList<FieldEntity>();
    this.views = new ArrayList<ViewEntity>();
    this.dependencies = new ArrayList<TableDependencyEntity>();
    this.transformationProperties = new ArrayList<TransformationEntity>();
    this.businessObjects = new ArrayList<BusinessObjectEntity>();
    this.tags = new ArrayList<String>();
  }

  public String getFqdn() {
    return fqdn;
  }

  public void setFqdn(String fqdn) {
    this.fqdn = fqdn;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getUrlPathPrefix() {
    return urlPathPrefix;
  }

  public void setUrlPathPrefix(String urlPathPrefix) {
    this.urlPathPrefix = urlPathPrefix;
  }

  public boolean isExternalTable() {
    return externalTable;
  }

  public void setExternalTable(boolean externalTable) {
    this.externalTable = externalTable;
  }

  public String getTableDescription() {
    return tableDescription;
  }

  public void setTableDescription(String tableDescription) {
    this.tableDescription = tableDescription;
  }

  public String getStorageFormat() {
    return storageFormat;
  }

  public void setStorageFormat(String storageFormat) {
    this.storageFormat = storageFormat;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public boolean isMaterializeOnce() {
    return materializeOnce;
  }

  public void setMaterializeOnce(boolean materializeOnce) {
    this.materializeOnce = materializeOnce;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  public String getTableOwner() {
    return tableOwner;
  }

  public void setTableOwner(String tableOwner) {
    this.tableOwner = tableOwner;
  }

  public String getDataPath() {
    return dataPath;
  }

  public void setDataPath(String dataPath) {
    this.dataPath = dataPath;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public long getRowcount() {
    return this.rowcount;
  }

  public void setRowcount(long rowcount) {
    this.rowcount = rowcount;
  }

  public String getTransformationType() {
    return transformationType;
  }

  public void setTransformationType(String transformationType) {
    this.transformationType = transformationType;
  }

  public String getTimestampField() {
    return timestampField;
  }

  public void setTimestampField(String timestampField) {
    this.timestampField = timestampField;
  }

  public String getTimestampFieldFormat() {
    return timestampFieldFormat;
  }

  public void setTimestampFieldFormat(String timestampFieldFormat) {
    this.timestampFieldFormat = timestampFieldFormat;
  }

  public String getPersonResponsible() {
    return personResponsible;
  }

  public void setPersonResponsible(String personResponsible) {
    this.personResponsible = personResponsible;
  }

  public void setFields(List<FieldEntity> fields) {
    this.fields = fields;
  }

  public List<ViewEntity> getViews() {
    return views;
  }

  public void setViews(List<ViewEntity> views) {
    this.views = views;
  }

  public List<TransformationEntity> getTransformationProperties() {
    return transformationProperties;
  }

  public void setTransformationProperties(List<TransformationEntity> transformationProperties) {
    this.transformationProperties = transformationProperties;
  }

  public List<BusinessObjectEntity> getBusinessObjects() {
    return businessObjects;
  }

  public void setBusinessObjects(List<BusinessObjectEntity> businessObjects) {
    this.businessObjects = businessObjects;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public int getViewCount() {
    return viewCount;
  }

  public void setViewCount(int viewCount) {
    this.viewCount = viewCount;
  }

  public List<TableDependencyEntity> getDependencies() {
    return dependencies;
  }

  public List<FieldEntity> getAllFieldEntities() {
    return this.fields;
  }

  public List<FieldEntity> getFields() {
    List<FieldEntity> f = new ArrayList<FieldEntity>();
    if (fields != null) {
      for (FieldEntity tableField : fields) {
        if (!tableField.isParameterField()) {
          f.add(tableField);
        }
      }
    }
    return f;
  }

  public List<FieldEntity> getParameters() {
    List<FieldEntity> parameters = new ArrayList<FieldEntity>();
    if (fields != null) {
      for (FieldEntity tableField : fields) {
        if (tableField.isParameterField()) {
          parameters.add(tableField);
        }
      }
    }
    return parameters;
  }

  public boolean isPartitioned() {
    return getParameters().size() > 0;
  }

  public List<String> getFieldNames() {
    List<String> result = new ArrayList<String>();
    if (fields == null) {
      return result;
    }
    for (FieldEntity field : fields) {
      result.add(field.getName());
    }
    return result;
  }

  public List<String> getCategoryNames() {
    List<String> categories = new ArrayList<String>();
    List<BusinessObjectEntity> bos = getBusinessObjects();
    if (bos != null) {
      for (BusinessObjectEntity businessObjectEntity : bos) {
        if (!categories.contains(businessObjectEntity.getCategoryName())) {
          categories.add(businessObjectEntity.getCategoryName());
        }
      }
    }
    return categories;
  }

  public List<String> getBusinessObjectNames() {
    List<String> result = new ArrayList<String>();
    if (businessObjects == null) {
      return result;
    }
    for (BusinessObjectEntity boEntity : businessObjects) {
      result.add(boEntity.getName());
    }
    return result;
  }

  public String getFieldsCommaDelimited() {
    String result = "";
    if (fields != null) {
      for (FieldEntity field : fields) {
        if (!field.isParameterField()) {
          if (!result.isEmpty()) {
            result += ", ";
          }
          result += field.getName();
        }
      }
    }
    return result;
  }

  public String getParameterCommaDelimited() {
    String result = "";
    for (FieldEntity field : fields) {
      if (field.isParameterField()) {
        if (!result.isEmpty()) {
          result += ",";
        }
        result += field.getName();
      }
    }
    return result;
  }

  public String getCategoriesCommaDelimited() {
    String result = "";
    List<String> categoryNames = getCategoryNames();
    if (categoryNames != null) {
      for (String categoryName : categoryNames) {
        if (!result.isEmpty()) {
          result += ",";
        }
        result += categoryName;
      }
    }
    return result;
  }

  public String getBusinessObjectsCommaDelimited() {
    String result = "";
    List<BusinessObjectEntity> bos = getBusinessObjects();
    if (bos != null) {
      for (BusinessObjectEntity businessObject : bos) {
        if (!result.isEmpty()) {
          result += ",";
        }
        result += businessObject.getName();
      }
    }
    return result;
  }

  public String getTagsCommaDelimited() {
    String result = "";
    List<String> tags = getTags();
    if (tags != null) {
      for (String tag : tags) {
        if (!result.isEmpty()) {
          result += ",";
        }
        result += tag;
      }
    }
    return result;
  }

  public String getParameterString() {
    String parameterString = "/";
    for (FieldEntity tableField : getParameters()) {
      parameterString += tableField.getName() + "/";
    }
    return parameterString;
  }

  public void setLastTransformation(long ts) {
    this.lastTransformationTimestamp = ts;
  }

  public long getLastTransformation() {
    return lastTransformationTimestamp;
  }

  public void setLastChange(long time) {
    this.lastChange = time;
  }

  public long getLastChange() {
    return lastChange;
  }

  public void setLastSchemaChange(long time) {
    this.lastSchemaChange = time;
  }

  public long getLastSchemaChange() {
    return lastSchemaChange;
  }

  public void addToFields(FieldEntity fieldEntity) {
    if (this.fields == null) {
      this.fields = new ArrayList<FieldEntity>();
    }
    this.fields.add(fieldEntity);
  }

  public void removeFromFields(FieldEntity fieldEntity) {
    this.fields.remove(fieldEntity);
  }

  public void addToTransformationProperties(TransformationEntity te) {
    if (this.transformationProperties == null) {
      this.transformationProperties = new ArrayList<TransformationEntity>();
    }
    this.transformationProperties.add(te);
  }

  public void setLastData(String lastData) {
    this.lastData = lastData;
  }

  public String getLastData() {
    return lastData;
  }

  public long getLastPartitionCreated() {
    return lastPartitionCreated;
  }

  public void setLastPartitionCreated(long lastPartitionCreated) {
    this.lastPartitionCreated = lastPartitionCreated;
  }

}
