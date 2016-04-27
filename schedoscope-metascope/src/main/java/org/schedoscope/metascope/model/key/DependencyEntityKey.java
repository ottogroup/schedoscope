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
package org.schedoscope.metascope.model.key;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.schedoscope.metascope.model.ViewDependencyEntity;

@Embeddable
public class DependencyEntityKey implements Serializable {

  private static final long serialVersionUID = 4318941563721216802L;

  @Column(name = ViewDependencyEntity.URL_PATH)
  private String urlPath;
  @Column(name = ViewDependencyEntity.DEPENDENCY_URL_PATH)
  private String dependencyUrlPath;
  @Column(name = ViewDependencyEntity.DEPENDENCY_FQDN)
  private String dependencyFqdn;

  public DependencyEntityKey() {
  }

  public DependencyEntityKey(String urlPath, String dependencyUrlPath, String dependencyFqdn) {
    this.urlPath = urlPath;
    this.dependencyUrlPath = dependencyUrlPath;
    this.dependencyFqdn = dependencyFqdn;
  }

  public String getUrlPath() {
    return urlPath;
  }

  public void setUrlPath(String urlPath) {
    this.urlPath = urlPath;
  }

  public String getDependencyUrlPath() {
    return dependencyUrlPath;
  }

  public void setDependencyUrlPath(String dependencyUrlPath) {
    this.dependencyUrlPath = dependencyUrlPath;
  }

  public String getDependencyFqdn() {
    return dependencyFqdn;
  }

  public void setDependencyFqdn(String depdencyFqdn) {
    this.dependencyFqdn = depdencyFqdn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dependencyFqdn == null) ? 0 : dependencyFqdn.hashCode());
    result = prime * result + ((dependencyUrlPath == null) ? 0 : dependencyUrlPath.hashCode());
    result = prime * result + ((urlPath == null) ? 0 : urlPath.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DependencyEntityKey other = (DependencyEntityKey) obj;
    if (dependencyFqdn == null) {
      if (other.dependencyFqdn != null)
        return false;
    } else if (!dependencyFqdn.equals(other.dependencyFqdn))
      return false;
    if (dependencyUrlPath == null) {
      if (other.dependencyUrlPath != null)
        return false;
    } else if (!dependencyUrlPath.equals(other.dependencyUrlPath))
      return false;
    if (urlPath == null) {
      if (other.urlPath != null)
        return false;
    } else if (!urlPath.equals(other.urlPath))
      return false;
    return true;
  }

}
