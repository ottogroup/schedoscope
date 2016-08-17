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

import org.schedoscope.metascope.model.key.TableDependencyEntityKey;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

@Entity
public class TableDependencyEntity {

    public static final String FQDN = "fqdn";
    public static final String DEPENDENCY_FQDN = "dependency_fqdn";

    @EmbeddedId
    private TableDependencyEntityKey key;

    public TableDependencyEntity() {
        this.key = new TableDependencyEntityKey();
    }

    public TableDependencyEntity(String fqdn, String depdencyFqdn) {
        if (key == null) {
            this.key = new TableDependencyEntityKey(fqdn, depdencyFqdn);
        } else {
            setFqdn(fqdn);
            setDependencyFqdn(depdencyFqdn);
        }
    }

    public TableDependencyEntityKey getEntityKey() {
        return key;
    }

    public String getFqdn() {
        return this.key.getFqdn();
    }

    public void setFqdn(String fqdn) {
        this.key.setFqdn(fqdn);
    }

    public String getDependencyFqdn() {
        return this.key.getDependencyFqdn();
    }

    public void setDependencyFqdn(String fqdn) {
        this.key.setDependencyFqdn(fqdn);
    }

}
