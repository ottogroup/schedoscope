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

import org.schedoscope.metascope.model.key.ParameterValueEntityKey;

@Entity
public class ParameterValueEntity {

  public static final String URL_PATH = "url_path";
  public static final String KEY = "p_key";
  public static final String VALUE = "value";
  public static final String TABLE_FQDN = "table_fqdn";

  @EmbeddedId
  private ParameterValueEntityKey key;
  @Column(name = VALUE)
  private String value;
  @Column(name = TABLE_FQDN)
  private String tableFqdn;

  public ParameterValueEntity() {
    this.key = new ParameterValueEntityKey();
  }

  public ParameterValueEntity(String urlPath, String tableFqdn, String p_key, String value) {
    if (key == null) {
      key = new ParameterValueEntityKey(urlPath, p_key);
    } else {
      setUrlPath(urlPath);
      setKey(p_key);
    }
    this.value = value;
    this.tableFqdn = tableFqdn;
  }

  public ParameterValueEntityKey getEntityKey() {
    return key;
  }

  public String getUrlPath() {
    return key.getUrlPath();
  }

  public void setUrlPath(String urlPath) {
    this.key.setUrlPath(urlPath);
  }

  public String getKey() {
    return key.getKey();
  }

  public void setKey(String key) {
    this.key.setKey(key);
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setTableFqdn(String tableFqdn) {
    this.tableFqdn = tableFqdn;
  }

  public String getTableFqdn() {
    return tableFqdn;
  }

}
