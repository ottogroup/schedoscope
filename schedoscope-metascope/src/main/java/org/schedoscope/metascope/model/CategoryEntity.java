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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;

@Entity
public class CategoryEntity {

  @Id
  private String categoryName;

  @OneToMany(mappedBy = "categoryName")
  private List<BusinessObjectEntity> businessObjects;

  public CategoryEntity() {
    this.businessObjects = new ArrayList<BusinessObjectEntity>();
  }

  public String getCategoryName() {
    return categoryName;
  }

  public void setCategoryName(String categoryName) {
    this.categoryName = categoryName;
  }

  public List<BusinessObjectEntity> getBusinessObjects() {
    return businessObjects;
  }

  public void setBusinessObjects(List<BusinessObjectEntity> businessObjects) {
    this.businessObjects = businessObjects;
  }

}
