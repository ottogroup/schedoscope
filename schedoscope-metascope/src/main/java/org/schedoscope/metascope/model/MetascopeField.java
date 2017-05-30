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

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
public class MetascopeField extends Documentable {

    /* fields */
    @Id
    private String fieldId;
    private String fieldName;
    @Column(columnDefinition = "varchar(31000)")
    private String fieldType;
    private int fieldOrder;
    @Column(columnDefinition = "varchar(31000)")
    private String description;
    private boolean isParameter;
    @ManyToOne(fetch = FetchType.LAZY)
    private MetascopeTable table;
    @Transient
    private Long commentId;
    @Transient
    private String tableFqdn;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "metascope_field_relationship",
            joinColumns = @JoinColumn(name = "dependency"),
            inverseJoinColumns = @JoinColumn(name = "successor"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"dependency", "successor"})
    )
    private List<MetascopeField> dependencies;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "metascope_field_relationship",
            joinColumns = @JoinColumn(name = "successor"),
            inverseJoinColumns = @JoinColumn(name = "dependency"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"dependency", "successor"})
    )
    private List<MetascopeField> successors;

    /* getter and setter */
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFieldId() {
        return fieldId;
    }

    public void setFieldId(String fieldId) {
        this.fieldId = fieldId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public MetascopeTable getTable() {
        return table;
    }

    public void setTable(MetascopeTable table) {
        this.table = table;
    }

    public int getFieldOrder() {
        return fieldOrder;
    }

    public void setFieldOrder(int fieldOrder) {
        this.fieldOrder = fieldOrder;
    }

    public boolean isParameter() {
        return isParameter;
    }

    public void setParameter(boolean parameter) {
        isParameter = parameter;
    }

    public List<MetascopeField> getDependencies() {
        return dependencies;
    }

    public List<MetascopeField> getSuccessors() {
        return successors;
    }

    public Long getCommentId() {
        return commentId;
    }

    public String getTableFqdn() {
        return tableFqdn;
    }

    public void setCommentId(Long commentId) {
        this.commentId = commentId;
    }

    public void setTableFqdn(String tableFqdn) {
        this.tableFqdn = tableFqdn;
    }

    public void addToDependencies(MetascopeField field) {
        if (dependencies == null) {
            this.dependencies = new ArrayList<>();
        }
        if (!dependencies.contains(field)) {
            this.dependencies.add(field);
        }
    }

    public void addToSuccessors(MetascopeField field) {
        if (successors == null) {
            this.successors = new ArrayList<>();
        }
        if (!successors.contains(field)) {
            this.successors.add(field);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetascopeField that = (MetascopeField) o;

        return fieldId.equals(that.fieldId);

    }

    @Override
    public int hashCode() {
        return fieldId.hashCode();
    }

}