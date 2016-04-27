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

import org.schedoscope.metascope.model.key.TransformationEntityKey;

@Entity
public class TransformationEntity {

  public static final String FQDN = "fqdn";
  public static final String KEY = "t_key";
  public static final String VALUE = "t_value";
  public static final String TABLE_FQDN = "table_fqdn";

  @EmbeddedId
  private TransformationEntityKey key;
  @Column(name = VALUE, columnDefinition = "varchar(32672)")
  private String tValue;

  @ManyToOne(optional = false, fetch = FetchType.LAZY)
  @JoinColumn(name = TABLE_FQDN, nullable = false)
  private TableEntity table;

  public TransformationEntity() {
    this.key = new TransformationEntityKey();
  }

  public TransformationEntity(String fqdn, String key, String value) {
    if (this.key == null) {
      this.key = new TransformationEntityKey(fqdn, key);
    } else {
      setFqdn(fqdn);
      setTransformationKey(key);
    }
    this.tValue = value;
  }

  public TransformationEntityKey getEntityKey() {
    return key;
  }

  public TableEntity getTable() {
    return table;
  }

  public void setTable(TableEntity table) {
    this.table = table;
  }

  public String getFqdn() {
    return this.key.getFqdn();
  }

  public void setFqdn(String fqdn) {
    this.key.setFqdn(fqdn);
  }

  public String getTransformationKey() {
    return this.key.getTransformationKey();
  }

  public void setTransformationKey(String key) {
    this.key.setTransformationKey(key);
  }

  public String getTransformationValue() {
    return tValue;
  }

  public void setTransformationValue(String transformationValue) {
    this.tValue = transformationValue;
  }

}
