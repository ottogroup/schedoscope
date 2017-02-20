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
