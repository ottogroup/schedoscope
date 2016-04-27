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

import org.schedoscope.metascope.model.ParameterValueEntity;

@Embeddable
public class ParameterValueEntityKey implements Serializable {

  private static final long serialVersionUID = 3008056897508198198L;

  @Column(name = ParameterValueEntity.URL_PATH)
  private String urlPath;
  @Column(name = ParameterValueEntity.KEY)
  private String pKey;

  public ParameterValueEntityKey() {
  }

  public ParameterValueEntityKey(String urlPath, String key) {
    this.urlPath = urlPath;
    this.pKey = key;
  }

  public String getUrlPath() {
    return urlPath;
  }

  public void setUrlPath(String urlPath) {
    this.urlPath = urlPath;
  }

  public String getKey() {
    return pKey;
  }

  public void setKey(String key) {
    this.pKey = key;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((pKey == null) ? 0 : pKey.hashCode());
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
    ParameterValueEntityKey other = (ParameterValueEntityKey) obj;
    if (pKey == null) {
      if (other.pKey != null)
        return false;
    } else if (!pKey.equals(other.pKey))
      return false;
    if (urlPath == null) {
      if (other.urlPath != null)
        return false;
    } else if (!urlPath.equals(other.urlPath))
      return false;
    return true;
  }

}
