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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.BusinessObjectEntity;
import org.schedoscope.metascope.model.CategoryEntity;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaxonomyServiceTest extends SpringTest {

  private static final String TEST_BUSINESS_OBJECT_DESC_1 = "Business Object description";
  private static final String TEST_BUSINESS_OBJECT_DESC_2 = "some other Business Object description";

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_01_createCategory() {
    int categories = size(categoryEntityRepository.findAll());

    assertEquals(categories, 0);

    taxonomyService.createCategory(TEST_CATEGORY);

    Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();

    assertEquals(size(categoryEntities), 1);

    CategoryEntity categoryEntity = categoryEntities.iterator().next();

    assertEquals(categoryEntity.getCategoryName(), TEST_CATEGORY);
    assertEquals(categoryEntity.getBusinessObjects().size(), 0);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_02_createCategoryToBeDeleted() {
    int categories = size(categoryEntityRepository.findAll());

    assertEquals(categories, 1);

    taxonomyService.createCategory(TO_BE_DELETED);

    Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();

    assertEquals(size(categoryEntities), 2);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_03_createBusinessObject() {
    int bos = size(boEntityRepository.findAll());

    assertEquals(bos, 0);

    taxonomyService.createBusinessObject(TEST_CATEGORY, TEST_BUSINESS_OBJECT, TEST_BUSINESS_OBJECT_DESC_1);

    Iterable<BusinessObjectEntity> boEntites = boEntityRepository.findAll();

    assertEquals(size(boEntites), 1);

    BusinessObjectEntity boEntity = boEntites.iterator().next();

    assertEquals(boEntity.getName(), TEST_BUSINESS_OBJECT);

    for (CategoryEntity categoryEntity : categoryEntityRepository.findAll()) {
      if (categoryEntity.getCategoryName().equals(TEST_CATEGORY)) {
        List<BusinessObjectEntity> boEntities = categoryEntity.getBusinessObjects();
        assertEquals(boEntities.size(), 1);
        assertEquals(boEntites.iterator().next().getName(), TEST_BUSINESS_OBJECT);
      }
    }
  }

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_04_createBusinessObjectToBeDeleted() {
    int bos = size(boEntityRepository.findAll());

    assertEquals(bos, 1);

    taxonomyService.createBusinessObject(TO_BE_DELETED, TO_BE_DELETED, TEST_BUSINESS_OBJECT_DESC_1);

    Iterable<BusinessObjectEntity> boEntites = boEntityRepository.findAll();

    assertEquals(size(boEntites), 2);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_05_editBusinessObject() {
    BusinessObjectEntity boEntity = boEntityRepository.findByCategoryNameAndName(TO_BE_DELETED, TO_BE_DELETED);

    assertEquals(boEntity.getDescription(), TEST_BUSINESS_OBJECT_DESC_1);

    taxonomyService.editBusinessObject(TO_BE_DELETED, TO_BE_DELETED, TO_BE_DELETED, TO_BE_DELETED,
        TEST_BUSINESS_OBJECT_DESC_2);

    boEntity = boEntityRepository.findByCategoryNameAndName(TO_BE_DELETED, TO_BE_DELETED);

    assertEquals(boEntity.getDescription(), TEST_BUSINESS_OBJECT_DESC_2);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_06_deleteBusinessObject() {
    int bos = size(boEntityRepository.findAll());

    assertEquals(bos, 2);

    taxonomyService.deleteBusinessObject(TO_BE_DELETED, TO_BE_DELETED);

    Iterable<BusinessObjectEntity> boEntites = boEntityRepository.findAll();

    assertEquals(size(boEntites), 1);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void taxonomyService_07_deleteCategory() {
    int categories = size(categoryEntityRepository.findAll());

    assertEquals(categories, 2);

    taxonomyService.deleteCategory(TO_BE_DELETED);

    Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();

    assertEquals(size(categoryEntities), 1);
  }

}
