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
package org.schedoscope.metascope.model.key;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
public class ViewDataDistributionEntityKey implements Serializable {

    private static final long serialVersionUID = 3005056897508298198L;

    @Column
    private String urlPath;
    @Column
    private String fieldName;
    @Column
    private String distributionKey;

    public ViewDataDistributionEntityKey() {
    }

    public ViewDataDistributionEntityKey(String urlPath, String fieldName, String key) {
        this.urlPath = urlPath;
        this.fieldName = fieldName;
        this.distributionKey = key;
    }

    public String getUrlPath() {
        return urlPath;
    }

    public void setUrlPath(String urlPath) {
        this.urlPath = urlPath;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getDistributionKey() {
        return distributionKey;
    }

    public void setDistributionKey(String distributionKey) {
        this.distributionKey = distributionKey;
    }

    @Override
    public String toString() {
        return urlPath + " " + fieldName + " " + distributionKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((distributionKey == null) ? 0 : distributionKey.hashCode());
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
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
        ViewDataDistributionEntityKey other = (ViewDataDistributionEntityKey) obj;
        if (distributionKey == null) {
            if (other.distributionKey != null)
                return false;
        } else if (!distributionKey.equals(other.distributionKey))
            return false;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (urlPath == null) {
            if (other.urlPath != null)
                return false;
        } else if (!urlPath.equals(other.urlPath))
            return false;
        return true;
    }

}
