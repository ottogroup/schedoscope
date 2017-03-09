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
package org.schedoscope.metascope.util.model;

import org.schedoscope.metascope.model.MetascopeCategory;
import org.schedoscope.metascope.model.MetascopeCategoryObject;

import java.util.ArrayList;
import java.util.List;

public class CategoryMap {

    private List<MetascopeCategory> categories;
    private List<MetascopeCategoryObject> categoryObjects;

    public CategoryMap() {
        this.categories = new ArrayList<>();
        this.categoryObjects = new ArrayList<>();
    }

    public List<MetascopeCategory> getCategories() {
        return categories;
    }

    public List<MetascopeCategoryObject> getCategoryObjects() {
        return categoryObjects;
    }

    public void addToCategories(MetascopeCategory categorie) {
        if (!categories.contains(categorie)) {
            categories.add(categorie);
        }
    }

    public void addToCategoryObjects(MetascopeCategoryObject categoryObject) {
        if (!categoryObjects.contains(categoryObject)) {
            categoryObjects.add(categoryObject);
        }
    }

    public String getCategoriesAsString() {
        String res = "";
        for (MetascopeCategory category : categories) {
            if (!res.isEmpty()) {
                res += ",";
            }
            res += category.getName();
        }
        return res;
    }

    public String getCategoryObjectsAsString() {
        String res = "";
        for (MetascopeCategoryObject categoryObject : categoryObjects) {
            if (!res.isEmpty()) {
                res += ",";
            }
            res += categoryObject.getName();
        }
        return res;
    }

}
