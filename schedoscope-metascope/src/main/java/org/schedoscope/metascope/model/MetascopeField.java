package org.schedoscope.metascope.model;

import javax.persistence.*;

/**
 * Created by kas on 22.11.16.
 */
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
