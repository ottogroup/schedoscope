/**
 * Copyright 2017 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.task.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class View {

  @JsonProperty("viewPath")
  private String name;
  @JsonProperty("viewTableName")
  private String fqdn;
  private String tableName;
  private String database;
  private boolean isTable;
  private boolean managed;
  private String status;
  private List<ViewField> fields;
  private List<ViewField> parameters;
  private Map<String, List<String>> lineage;
  private Map<String, List<String>> dependencies;
  private ViewTransformation transformation;
  private List<ViewTransformation> export;
  private String storageFormat;
  private boolean external;
  private boolean materializeOnce;
  private String comment;
  private long createdAt;
  private String path;
  private String owner;
  private String inputFormat;
  private String outputFormat;
  private long size;
  private Map<String, String> properties;

  public View() {
    this.fields = new ArrayList<ViewField>();
    this.parameters = new ArrayList<ViewField>();
    this.dependencies = new HashMap<String, List<String>>();
    this.lineage = new HashMap<String, List<String>>();
  }

  public String getName() {
    return name;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getFqdn() {
    return fqdn;
  }

  public void setFqdn(String fqdn) {
    this.fqdn = fqdn;
    setTableAndDatabase();
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isTable() {
    return isTable;
  }

  public void setIsTable(boolean isTable) {
    this.isTable = isTable;
  }

  public boolean isManaged() {
    return managed;
  }

  public void setManaged(boolean managed) {
    this.managed = managed;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public List<ViewField> getFields() {
    return fields;
  }

  public void setFields(List<ViewField> fields) {
    this.fields = fields;
  }

  public List<ViewField> getParameters() {
    return parameters;
  }

  public void setParameters(List<ViewField> parameters) {
    this.parameters = parameters;
  }

  public Map<String, List<String>> getLineage() {
    return lineage;
  }

  public void setLineage(Map<String, List<String>> lineage) {
    this.lineage = lineage;
  }

  public Map<String, List<String>> getDependencies() {
    return dependencies;
  }

  public void setDependencies(Map<String, List<String>> dependencies) {
    this.dependencies = dependencies;
  }

  public ViewTransformation getTransformation() {
    return transformation;
  }

  public void setTransformation(ViewTransformation transformation) {
    this.transformation = transformation;
  }

  public List<ViewTransformation> getExport() {
    return export;
  }

  public void setExport(List<ViewTransformation> export) {
    this.export = export;
  }

  public String getStorageFormat() {
    return storageFormat;
  }

  public void setStorageFormat(String storageFormat) {
    this.storageFormat = storageFormat;
  }

  public boolean isExternal() {
    return external;
  }

  public void setExternal(boolean external) {
    this.external = external;
  }

  public boolean isMaterializeOnce() {
    return materializeOnce;
  }

  public void setMaterializeOnce(boolean materializeOnce) {
    this.materializeOnce = materializeOnce;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
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

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setTableAndDatabase() {
    setTableName(toTableName(fqdn));
    setDatabase(toDatabaseName(fqdn));
  }

  private String toTableName(String fqdn) {
    int index = fqdn.indexOf(".") + 1;
    return fqdn.substring(index);
  }

  private String toDatabaseName(String fqdn) {
    int firstSlash = fqdn.indexOf(".");
    return fqdn.substring(0, firstSlash);
  }

  public String viewPath() {
    int index = StringUtils.ordinalIndexOf(name, "/", 2) + 1;
    return name.substring(0, index);
  }

  public String getViewId() {
    int index = StringUtils.ordinalIndexOf(name, "/", 2) + 1;
    String[] parameterValues = name.substring(index).split("/");
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