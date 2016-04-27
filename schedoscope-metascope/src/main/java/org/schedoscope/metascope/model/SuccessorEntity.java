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

import org.schedoscope.metascope.model.key.SuccessorEntityKey;

@Entity
public class SuccessorEntity {

  public static final String URL_PATH = "url_path";
  public static final String SUCCESSOR_URL_PATH = "successor_url_path";
  public static final String SUCCESSOR_FQDN = "successor_fqdn";
  public static final String INTERNAL_VIEW_ID = "internal_view_id";

  @EmbeddedId
  private SuccessorEntityKey key;
  @Column(name = SuccessorEntity.INTERNAL_VIEW_ID)
  private String internalViewId;

  public SuccessorEntity() {
    this.key = new SuccessorEntityKey();
  }

  public SuccessorEntity(String urlPath, String successorUrlPath, String depdencyFqdn) {
    if (key == null) {
      this.key = new SuccessorEntityKey(urlPath, successorUrlPath, depdencyFqdn);
    } else {
      setUrlPath(urlPath);
      setSuccessorUrlPath(successorUrlPath);
      setSuccessorFqdn(depdencyFqdn);
    }
  }

  public SuccessorEntityKey getEntityKey() {
    return key;
  }

  public String getUrlPath() {
    return this.key.getUrlPath();
  }

  public void setUrlPath(String urlPath) {
    this.key.setUrlPath(urlPath);
  }

  public String getSuccessorUrlPath() {
    return this.key.getSuccessorUrlPath();
  }

  public void setSuccessorUrlPath(String successorUrlPath) {
    this.key.setSuccessorUrlPath(successorUrlPath);
  }

  public String getSuccessorFqdn() {
    return this.key.getSuccessorFqdn();
  }

  public void setSuccessorFqdn(String fqdn) {
    this.key.setSuccessorFqdn(fqdn);
  }

  public String getInternalViewId() {
    return internalViewId;
  }

  public void setInternalViewId(String internalViewId) {
    this.internalViewId = internalViewId;
  }

}
