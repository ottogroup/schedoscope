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

import org.schedoscope.metascope.model.TransformationEntity;

@Embeddable
public class TransformationEntityKey implements Serializable {

  private static final long serialVersionUID = 8650002554712200504L;

  @Column(name = TransformationEntity.FQDN)
  private String fqdn;
  @Column(name = TransformationEntity.KEY)
  private String tKey;

  public TransformationEntityKey() {
  }

  public TransformationEntityKey(String fqdn, String key) {
    this.fqdn = fqdn;
    this.tKey = key;
  }

  public String getFqdn() {
    return fqdn;
  }

  public void setFqdn(String fqdn) {
    this.fqdn = fqdn;
  }

  public String getTransformationKey() {
    return tKey;
  }

  public void setTransformationKey(String transformationKey) {
    this.tKey = transformationKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fqdn == null) ? 0 : fqdn.hashCode());
    result = prime * result + ((tKey == null) ? 0 : tKey.hashCode());
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
    TransformationEntityKey other = (TransformationEntityKey) obj;
    if (fqdn == null) {
      if (other.fqdn != null)
        return false;
    } else if (!fqdn.equals(other.fqdn))
      return false;
    if (tKey == null) {
      if (other.tKey != null)
        return false;
    } else if (!tKey.equals(other.tKey))
      return false;
    return true;
  }

}
