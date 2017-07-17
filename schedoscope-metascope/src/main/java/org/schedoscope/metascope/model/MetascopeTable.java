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
package org.schedoscope.metascope.model;

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Entity
public class MetascopeTable extends Documentable {

    /* fields */
    @Id
    private String fqdn;
    private String schedoscopeId;
    private String databaseName;
    private String tableName;
    private String viewPath;
    private boolean externalTable;
    @Column(columnDefinition = "text")
    private String tableDescription;
    private String storageFormat;
    private String inputFormat;
    private String outputFormat;
    private boolean materializeOnce;
    @Column(columnDefinition = "bigint default 0")
    private long createdAt;
    private String tableOwner;
    @Column(columnDefinition = "text")
    private String dataPath;
    @Column(columnDefinition = "bigint default 0")
    private long dataSize;
    private String permissions;
    @Column(columnDefinition = "bigint default 0")
    private long rowcount;
    private String lastData;
    private String timestampField;
    private String timestampFieldFormat;
    @Column(columnDefinition = "bigint default 0")
    private long lastChange;
    @Column(columnDefinition = "bigint default 0")
    private long lastPartitionCreated;
    @Column(columnDefinition = "bigint default 0")
    private long lastSchemaChange;
    @Column(columnDefinition = "bigint default 0")
    private long lastTransformationTimestamp;
    @Column(columnDefinition = "int default 0")
    private int viewCount;
    @Column(columnDefinition = "int default 1")
    private int viewsSize;
    private String personResponsible;
    @Transient
    private Long commentId;

    @OneToOne(mappedBy = "table", cascade = CascadeType.ALL)
    @LazyCollection(LazyCollectionOption.FALSE)
    private MetascopeTransformation transformation;

    @ElementCollection
    @LazyCollection(LazyCollectionOption.FALSE)
    private List<String> tags;

    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @OrderBy(value = "fieldOrder")
    @JoinTable(name = "metascope_fields_mapping")
    private Set<MetascopeField> fields;

    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @OrderBy(value = "fieldOrder")
    @JoinTable(name = "metascope_parameter_mapping")
    @LazyCollection(LazyCollectionOption.FALSE)
    private Set<MetascopeField> parameters;

    @OneToMany(cascade = CascadeType.ALL)
    @LazyCollection(LazyCollectionOption.FALSE)
    private List<MetascopeExport> exports;

    @ManyToMany(cascade = CascadeType.ALL)
    @LazyCollection(LazyCollectionOption.FALSE)
    private List<MetascopeCategoryObject> categoryObjects;

    @OneToMany(mappedBy = "table", cascade = CascadeType.ALL)
    @LazyCollection(LazyCollectionOption.FALSE)
    private List<MetascopeActivity> activities;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "table", cascade = CascadeType.ALL)
    private List<MetascopeView> views;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "metascope_table_relationship",
      joinColumns = @JoinColumn(name = "successor"),
      inverseJoinColumns = @JoinColumn(name = "dependency"),
      uniqueConstraints = @UniqueConstraint(columnNames = {"dependency", "successor"})
    )
    private List<MetascopeTable> successors;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "metascope_table_relationship",
      joinColumns = @JoinColumn(name = "dependency"),
      inverseJoinColumns = @JoinColumn(name = "successor"),
      uniqueConstraints = @UniqueConstraint(columnNames = {"dependency", "successor"})
    )
    private List<MetascopeTable> dependencies;

    /* ### GETTER/SETTER START ### */
    public Set<MetascopeField> getParameters() {
        return parameters;
    }

    public void setParameters(Set<MetascopeField> parameters) {
        this.parameters = parameters;
    }

    public List<MetascopeExport> getExports() {
        return exports;
    }

    public void setExports(List<MetascopeExport> exports) {
        this.exports = exports;
    }

    public String getFqdn() {
        return fqdn;
    }

    public void setFqdn(String fqdn) {
        this.fqdn = fqdn;
    }

    public String getSchedoscopeId() {
        return schedoscopeId;
    }

    public void setSchedoscopeId(String schedoscopeId) {
        this.schedoscopeId = schedoscopeId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Set<MetascopeField> getFields() {
        return fields;
    }

    public MetascopeField getField(String fieldFqdn) {
        for (MetascopeField field : fields) {
            if (field.getFieldId().equals(fieldFqdn)) {
                return field;
            }
        }
        return null;
    }

    public void setFields(Set<MetascopeField> fields) {
        this.fields = fields;
    }

    public boolean isExternalTable() {
        return externalTable;
    }

    public void setExternalTable(boolean externalTable) {
        this.externalTable = externalTable;
    }

    public String getViewPath() {
        return viewPath;
    }

    public void setViewPath(String viewPath) {
        this.viewPath = viewPath;
    }

    public String getPersonResponsible() {
        return personResponsible;
    }

    public void setPersonResponsible(String personResponsible) {
        this.personResponsible = personResponsible;
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

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    public long getRowcount() {
        return rowcount;
    }

    public void setRowcount(long rowcount) {
        this.rowcount = rowcount;
    }

    public String getLastData() {
        return lastData;
    }

    public void setLastData(String lastData) {
        this.lastData = lastData;
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

    public long getLastChange() {
        return lastChange;
    }

    public void setLastChange(long lastChange) {
        this.lastChange = lastChange;
    }

    public long getLastPartitionCreated() {
        return lastPartitionCreated;
    }

    public void setLastPartitionCreated(long lastPartitionCreated) {
        this.lastPartitionCreated = lastPartitionCreated;
    }

    public long getLastSchemaChange() {
        return lastSchemaChange;
    }

    public void setLastSchemaChange(long lastSchemaChange) {
        this.lastSchemaChange = lastSchemaChange;
    }

    public long getLastTransformation() {
        return lastTransformationTimestamp;
    }

    public void setLastTransformation(long lastTransformationTimestamp) {
        this.lastTransformationTimestamp = lastTransformationTimestamp;
    }

    public int getViewCount() {
        return viewCount;
    }

    public void setViewCount(int viewCount) {
        this.viewCount = viewCount;
    }

    public List<MetascopeView> getViews() {
        return views;
    }

    public void setViews(List<MetascopeView> views) {
        this.views = views;
    }

    public List<MetascopeCategoryObject> getCategoryObjects() {
        return categoryObjects;
    }

    public void setCategoryObjects(List<MetascopeCategoryObject> categoryObjects) {
        this.categoryObjects = categoryObjects;
    }

    public MetascopeTransformation getTransformation() {
        return transformation;
    }

    public void setTransformation(MetascopeTransformation transformation) {
        this.transformation = transformation;
        if (transformation.getTable() == null) {
            transformation.setTable(this);
        }
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public List<MetascopeActivity> getActivities() {
        return activities;
    }

    public void setActivities(List<MetascopeActivity> activities) {
        this.activities = activities;
    }

    public List<MetascopeTable> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<MetascopeTable> dependencies) {
        this.dependencies = dependencies;
    }

    public List<MetascopeTable> getSuccessors() {
        return successors;
    }

    public void setSuccessors(List<MetascopeTable> successors) {
        this.successors = successors;
    }

    public int getViewsSize() {
        return viewsSize;
    }

    public void setViewsSize(int viewsSize) {
        this.viewsSize = viewsSize;
    }

    public Long getCommentId() {
        return commentId;
    }

    public void setCommentId(Long commentId) {
        this.commentId = commentId;
    }

    /* ### GETTER/SETTER END ### */

    /* ### ADD/REMOVE START ### */
    public void addToFields(MetascopeField field) {
        if (fields == null) {
            this.fields = new LinkedHashSet<>();
        }
        fields.remove(field);
        this.fields.add(field);
        field.setTable(this);
    }

    public void addToParameters(MetascopeField parameter) {
        if (parameters == null) {
            this.parameters = new LinkedHashSet<>();
        }
        parameters.remove(parameter);
        this.parameters.add(parameter);
        parameter.setTable(this);
    }

    public void addToExports(MetascopeExport export) {
        if (exports == null) {
            this.exports = new ArrayList<>();
        }
        exports.remove(export);
        this.exports.add(export);
        export.setTable(this);
    }

    public void addToViews(MetascopeView view) {
        if (views == null) {
            this.views = new ArrayList<>();
        }
        views.remove(view);
        this.views.add(view);
        view.setTable(this);
    }

    public void addToDependencies(MetascopeTable table) {
        if (dependencies == null) {
            this.dependencies = new ArrayList<>();
        }
        dependencies.remove(table);
        this.dependencies.add(table);
    }

    public void addToSuccessor(MetascopeTable table) {
        if (successors == null) {
            this.successors = new ArrayList<>();
        }
        successors.remove(table);
        this.successors.add(table);
    }

    public void addToTags(String tag) {
        if (tags == null) {
            this.tags = new ArrayList<>();
        }
        this.tags.add(tag);
    }

    public void removeCategoryObjectById(long categoryObjectId) {
        MetascopeCategoryObject toRemove = null;
        for (MetascopeCategoryObject categoryObjectEntity : categoryObjects) {
            if (categoryObjectEntity.getCategoryObjectId() == categoryObjectId) {
                toRemove = categoryObjectEntity;
            }
        }
        if (toRemove != null) {
            categoryObjects.remove(toRemove);
        }
    }

  /* ### ADD/REMOVE END ### */

  /* ### COLLECTION GETTER START ### */

    public List<String> getFieldNames() {
        List<String> result = new ArrayList<String>();
        if (fields == null) {
            return result;
        }
        for (MetascopeField field : fields) {
            result.add(field.getFieldName());
        }
        return result;
    }

    public List<String> getParameterNames() {
        List<String> result = new ArrayList<String>();
        if (parameters == null) {
            return result;
        }
        for (MetascopeField parameter : parameters) {
            result.add(parameter.getFieldName());
        }
        return result;
    }

    public List<MetascopeField> getOrderedParameters() {
        List<MetascopeField> ordered = new ArrayList<>();
        for (MetascopeField parameter : parameters) {
            ordered.add(parameter.getFieldOrder(), parameter);
        }
        return ordered;
    }

    public List<String> getExportNames() {
        List<String> result = new ArrayList<String>();
        if (exports == null) {
            return result;
        }
        for (MetascopeExport exportEntity : exports) {
            result.add(exportEntity.getExportType());
        }
        return result;
    }

    public List<String> getTaxonomyNames() {
        List<String> taxonomies = new ArrayList<String>();
        List<MetascopeCategoryObject> cos = getCategoryObjects();
        if (cos != null) {
            for (MetascopeCategoryObject categoryObjectEntity : cos) {
                MetascopeTaxonomy taxonomy = categoryObjectEntity.getCategory().getTaxonomy();
                String taxonomyName = taxonomy.getName();
                if (!taxonomies.contains(taxonomyName)) {
                    taxonomies.add(taxonomyName);
                }
            }
        }
        return taxonomies;
    }

    public List<String> getCategoryNames() {
        List<String> categories = new ArrayList<String>();
        List<MetascopeCategoryObject> cos = getCategoryObjects();
        if (cos != null) {
            for (MetascopeCategoryObject categoryObjectEntity : cos) {
                String categoryName = categoryObjectEntity.getCategory().getName();
                if (!categories.contains(categoryName)) {
                    categories.add(categoryName);
                }
            }
        }
        return categories;
    }

    public List<String> getCategoryObjectNames() {
        List<String> result = new ArrayList<String>();
        if (categoryObjects == null) {
            return result;
        }
        for (MetascopeCategoryObject coEntity : categoryObjects) {
            result.add(coEntity.getName());
        }
        return result;
    }

  /* ### COLLECTION GETTER END ### */

  /* ### HELPER START ###  */

    public boolean isPartitioned() {
        return getParameters().size() > 0;
    }

    public String getFieldsCommaDelimited() {
        String result = "";
        if (fields != null) {
            for (MetascopeField field : fields) {
                if (!result.isEmpty()) {
                    result += ", ";
                }
                result += field.getFieldName();
            }
        }
        return result;
    }

    public String getParameterCommaDelimited() {
        String result = "";
        for (MetascopeField field : parameters) {
            if (!result.isEmpty()) {
                result += ",";
            }
            result += field.getFieldName();
        }
        return result;
    }

    public String getTaxonomiesCommaDelimited() {
        String result = "";
        List<String> taxonomyNames = getTaxonomyNames();
        if (taxonomyNames != null) {
            for (String taxonomyName : taxonomyNames) {
                if (!result.isEmpty()) {
                    result += ",";
                }
                result += taxonomyName;
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

    public String getCategoryObjectsCommaDelimited() {
        String result = "";
        List<MetascopeCategoryObject> cos = getCategoryObjects();
        if (cos != null) {
            for (MetascopeCategoryObject categoryObject : cos) {
                if (!result.isEmpty()) {
                    result += ",";
                }
                result += categoryObject.getName();
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
        for (MetascopeField parameter : getParameters()) {
            parameterString += parameter.getFieldName() + "/";
        }
        return parameterString;
    }

    /* ### HELPER END ###  */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof MetascopeTable)) return false;

        MetascopeTable that = (MetascopeTable) o;

        return fqdn.equals(that.fqdn);

    }

    @Override
    public int hashCode() {
        return fqdn.hashCode();
    }


}
