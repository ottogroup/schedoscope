/**
 * Copyright 2015 Otto (GmbH & Co KG)
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

import org.schedoscope.metascope.model.key.ViewDataDistributionEntityKey;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

@Entity
public class DataDistributionEntity {

    @EmbeddedId
    private ViewDataDistributionEntityKey key;
    @Column(columnDefinition = "varchar(32672)")
    private String distributionValue;

    public DataDistributionEntity() {
    }

    public DataDistributionEntity(String urlPath, String fieldName, String c_key, String value) {
        if (key == null) {
            key = new ViewDataDistributionEntityKey(urlPath, fieldName, c_key);
        } else {
            setUrlPath(urlPath);
            setFieldName(fieldName);
            setDistributionKey(c_key);
        }
        this.distributionValue = value;
    }

    public String getUrlPath() {
        return this.key.getUrlPath();
    }

    public void setUrlPath(String urlPath) {
        this.key.setUrlPath(urlPath);
    }

    public String getFieldName() {
        return this.key.getFieldName();
    }

    public void setFieldName(String fieldName) {
        this.key.setFieldName(fieldName);
    }

    public String getDistributionKey() {
        return this.key.getDistributionKey();
    }

    public void setDistributionKey(String distributionKey) {
        this.key.setDistributionKey(distributionKey);
    }

    public String getDistributionValue() {
        return distributionValue;
    }

    public void setDistributionValue(String distributionValue) {
        this.distributionValue = distributionValue;
    }

}
