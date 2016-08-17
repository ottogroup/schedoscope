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
import java.util.ArrayList;
import java.util.List;

@Entity
public class CategoryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long categoryId;

    @ManyToOne
    private TaxonomyEntity taxonomy;

    @Column(columnDefinition = "varchar(32672)")
    private String name;

    @OneToMany(mappedBy = "category", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
    private List<CategoryObjectEntity> categoryObjects;

    public CategoryEntity() {
        this.categoryObjects = new ArrayList<CategoryObjectEntity>();
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public TaxonomyEntity getTaxonomy() {
        return taxonomy;
    }

    public void setTaxonomy(TaxonomyEntity taxonomy) {
        this.taxonomy = taxonomy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<CategoryObjectEntity> getCategoryObjects() {
        return categoryObjects;
    }

    public void setCategoryObjects(List<CategoryObjectEntity> categoryObjects) {
        this.categoryObjects = categoryObjects;
    }

}
