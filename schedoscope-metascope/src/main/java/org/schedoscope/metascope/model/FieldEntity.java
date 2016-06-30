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

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import org.schedoscope.metascope.model.key.FieldEntityKey;

@Entity
public class FieldEntity extends Documentable {

  public static final String FQDN = "fqdn";
  public static final String NAME = "name";
  public static final String TYPE = "type";
  public static final String DESCRIPTION = "description";
  public static final String FIELD_ORDER = "field_order";
  public static final String PARAMETER_FIELD = "parameter_field";
  public static final String TABLE_FQDN = "table_fqdn";

  @EmbeddedId
  private FieldEntityKey key;
  @Column(name = TYPE, columnDefinition = "varchar(31000)")
  private String type;
  @Column(name = DESCRIPTION, columnDefinition = "varchar(31000)")
  private String description;
  @Column(name = FIELD_ORDER)
  private int fieldOrder;
  @Column(name = PARAMETER_FIELD)
  private boolean parameterField;

  @ManyToOne(optional = false, fetch = FetchType.LAZY)
  @JoinColumn(name = TABLE_FQDN, nullable = false)
  private TableEntity table;

  public FieldEntity() {
    this.key = new FieldEntityKey();
  }

  public FieldEntity(String fqdn, String name) {
    if (key == null) {
      key = new FieldEntityKey(fqdn, name);
    } else {
      setFqdn(fqdn);
      setName(name);
    }
  }

  public FieldEntityKey getEntityKey() {
    return key;
  }

  public TableEntity getTable() {
    return table;
  }

  public void setTable(TableEntity table) {
    this.table = table;
  }

  public String getFieldId() {
    return getFqdn() + "." + getName();
  }

  public String getFqdn() {
    return this.key.getFqdn();
  }

  public void setFqdn(String fqdn) {
    this.key.setFqdn(fqdn);
    ;
  }

  public String getName() {
    return this.key.getName();
  }

  public void setName(String name) {
    this.key.setName(name);
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDescription() {
    if (description == null) {
      return "";
    }
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public int getFieldOrder() {
    return fieldOrder;
  }

  public void setFieldOrder(int fieldOrder) {
    this.fieldOrder = fieldOrder;
  }

  public boolean isParameterField() {
    return parameterField;
  }

  public void setParameterField(boolean parameterField) {
    this.parameterField = parameterField;
  }

}
