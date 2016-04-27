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

import org.schedoscope.metascope.model.key.DependencyEntityKey;

@Entity
public class ViewDependencyEntity {

  public static final String URL_PATH = "url_path";
  public static final String DEPENDENCY_URL_PATH = "dependency_url_path";
  public static final String DEPENDENCY_FQDN = "dependency_fqdn";
  public static final String INTERNAL_VIEW_ID = "internal_view_id";

  @EmbeddedId
  private DependencyEntityKey key;
  @Column(name = INTERNAL_VIEW_ID)
  private String internalViewId;

  public ViewDependencyEntity() {
    this.key = new DependencyEntityKey();
  }

  public ViewDependencyEntity(String urlPath, String dependencyUrlPath, String depdencyFqdn) {
    if (key == null) {
      this.key = new DependencyEntityKey(urlPath, dependencyUrlPath, depdencyFqdn);
    } else {
      setUrlPath(urlPath);
      setDependencyUrlPath(dependencyUrlPath);
      setDependencyFqdn(depdencyFqdn);
    }
  }

  public DependencyEntityKey getEntityKey() {
    return key;
  }

  public String getUrlPath() {
    return this.key.getUrlPath();
  }

  public void setUrlPath(String urlPath) {
    this.key.setUrlPath(urlPath);
  }

  public String getDependencyUrlPath() {
    return this.key.getDependencyUrlPath();
  }

  public void setDependencyUrlPath(String dependencyUrlPath) {
    this.key.setDependencyUrlPath(dependencyUrlPath);
  }

  public String getDependencyFqdn() {
    return this.key.getDependencyFqdn();
  }

  public void setDependencyFqdn(String fqdn) {
    this.key.setDependencyFqdn(fqdn);
  }

  public String getInternalViewId() {
    return internalViewId;
  }

  public void setInternalViewId(String internalViewId) {
    this.internalViewId = internalViewId;
  }

}
