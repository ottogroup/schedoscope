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
package org.schedoscope.metascope.service;

import java.util.ArrayList;
import java.util.List;

import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.BusinessObjectEntity;
import org.schedoscope.metascope.model.CategoryEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.repository.BusinessObjectEntityRepository;
import org.schedoscope.metascope.repository.CategoryEntityRepository;
import org.schedoscope.metascope.repository.TableEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TaxonomyService {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyService.class);

  @Autowired
  private UserEntityService userEntityService;
  @Autowired
  private CategoryEntityRepository categoryEntityRepository;
  @Autowired
  private BusinessObjectEntityRepository businessObjectEntityRepository;
  @Autowired
  private TableEntityRepository tableEntityRepository;
  @Autowired
  private SolrFacade solr;

  public void createCategory(String categoryName) {
    if (categoryExists(categoryName)) {
      return;
    }

    CategoryEntity category = new CategoryEntity();
    category.setCategoryName(categoryName);
    categoryEntityRepository.save(category);
    LOG.info("User '{}' created new category '{}'", userEntityService.getUser().getUsername(), categoryName);
  }

  public void deleteCategory(String categoryName) {
    CategoryEntity category = this.categoryEntityRepository.findByCategoryName(categoryName);

    if (category == null) {
      return;
    }

    deleteBusinessObjects(category.getBusinessObjects());
    categoryEntityRepository.delete(category);
    LOG.info("User '{}' deleted category '{}'", userEntityService.getUser().getUsername(), categoryName);
  }

  public void createBusinessObject(String categoryName, String boName, String description) {
    CategoryEntity category = this.categoryEntityRepository.findByCategoryName(categoryName);

    if (category == null) {
      return;
    }

    BusinessObjectEntity bo = new BusinessObjectEntity();
    bo.setName(boName);
    bo.setDescription(description);
    bo.setCategoryName(category.getCategoryName());
    category.getBusinessObjects().add(bo);

    this.businessObjectEntityRepository.save(bo);
    this.categoryEntityRepository.save(category);
    LOG.info("User '{}' created new business object '{}'", userEntityService.getUser().getUsername(), boName);
  }

  public void editBusinessObject(String categoryName, String businessobjectName, String oldCategoryName,
      String oldBusinessobjectName, String description) {
    CategoryEntity oldCategpry = this.categoryEntityRepository.findByCategoryName(oldCategoryName);

    if (oldCategpry == null) {
      return;
    }

    CategoryEntity category = this.categoryEntityRepository.findByCategoryName(categoryName);

    BusinessObjectEntity bo = getBusinessObject(oldCategpry.getBusinessObjects(), oldBusinessobjectName);
    bo.setName(businessobjectName);
    bo.setDescription(description);
    bo.setCategoryName(categoryName);

    oldCategpry.getBusinessObjects().remove(bo);
    category.getBusinessObjects().add(bo);

    categoryEntityRepository.save(oldCategpry);
    categoryEntityRepository.save(category);
    businessObjectEntityRepository.save(bo);
    LOG.info("User '{}' changed business object name from '{}' to '{}' with category '{}'", userEntityService.getUser()
        .getUsername(), oldBusinessobjectName, businessobjectName, categoryName);
  }

  public void deleteBusinessObject(String categoryName, String businessobjectName) {
    CategoryEntity category = this.categoryEntityRepository.findByCategoryName(categoryName);

    if (category == null) {
      return;
    }

    BusinessObjectEntity bo = getBusinessObject(category.getBusinessObjects(), businessobjectName);

    if (bo == null) {
      return;
    }

    List<TableEntity> tables = tableEntityRepository.findByBusinessObject(bo);
    for (TableEntity tableEntity : tables) {
      BusinessObjectEntity toRemove = null;
      for (BusinessObjectEntity businessObjectEntity : tableEntity.getBusinessObjects()) {
        if (businessObjectEntity.getId() == bo.getId()) {
          toRemove = businessObjectEntity;
        }
      }
      tableEntity.getBusinessObjects().remove(toRemove);
      tableEntityRepository.save(tableEntity);
      solr.updateTableEntityAsync(tableEntity, true);
      LOG.info("User '{}' deleted business object '{}'", userEntityService.getUser().getUsername(), businessobjectName);
    }

    category.getBusinessObjects().remove(bo);
    categoryEntityRepository.save(category);
    businessObjectEntityRepository.delete(bo);
  }

  private void deleteBusinessObjects(List<BusinessObjectEntity> businessObjects) {
    List<Long> boIds = new ArrayList<Long>();
    for (BusinessObjectEntity businessObjectEntity : businessObjects) {
      boIds.add(businessObjectEntity.getId());
    }
    for (Long boId : boIds) {
      BusinessObjectEntity bo = businessObjectEntityRepository.findOne(boId);
      deleteBusinessObject(bo.getCategoryName(), bo.getName());
    }
  }

  public Iterable<CategoryEntity> getAllCategories() {
    return this.categoryEntityRepository.findAll();
  }

  private boolean categoryExists(String categoryName) {
    return categoryName != null && categoryEntityRepository.findByCategoryName(categoryName) != null;
  }

  private BusinessObjectEntity getBusinessObject(List<BusinessObjectEntity> businessObjects, String businessobjectName) {
    for (BusinessObjectEntity bo : businessObjects) {
      if (bo.getName().equals(businessobjectName)) {
        return bo;
      }
    }
    return null;
  }

}
