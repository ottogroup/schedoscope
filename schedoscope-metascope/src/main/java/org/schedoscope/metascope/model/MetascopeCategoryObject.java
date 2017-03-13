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

import javax.persistence.*;

@Entity
public class MetascopeCategoryObject {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long categoryObjectId;

    @ManyToOne
    private MetascopeCategory category;

    @Column(columnDefinition = "varchar(32672)")
    private String name;

    @Column(columnDefinition = "varchar(32672)")
    private String description;

    public long getCategoryObjectId() {
        return categoryObjectId;
    }

    public void setCategoryObjectId(long categoryObjectId) {
        this.categoryObjectId = categoryObjectId;
    }

    public MetascopeCategory getCategory() {
        return category;
    }

    public void setCategory(MetascopeCategory category) {
        this.category = category;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
