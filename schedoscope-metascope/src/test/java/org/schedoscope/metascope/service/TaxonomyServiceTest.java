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
package org.schedoscope.metascope.service;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.CategoryEntity;
import org.schedoscope.metascope.model.CategoryObjectEntity;
import org.schedoscope.metascope.model.TaxonomyEntity;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaxonomyServiceTest extends SpringTest {

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_01_createTaxonomy() {
        int taxonomies = size(taxonomyEntityRepository.findAll());

        assertEquals(taxonomies, 0);

        taxonomyService.createTaxonomy(TEST_TAXONOMY);

        Iterable<TaxonomyEntity> taxonomyEntities = taxonomyEntityRepository.findAll();

        assertEquals(size(taxonomyEntities), 1);

        TaxonomyEntity taxonomyEntity = taxonomyEntities.iterator().next();

        assertEquals(taxonomyEntity.getName(), TEST_TAXONOMY);
        assertEquals(taxonomyEntity.getCategories().size(), 0);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_02_createCategory() {
        Iterable<TaxonomyEntity> taxonomyEntities = taxonomyEntityRepository.findAll();
        int categories = size(categoryEntityRepository.findAll());

        assertEquals(categories, 0);

        taxonomyService.createCategory(taxonomyEntities.iterator().next().getTaxonomyId(), TEST_CATEGORY_NAME);

        Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();

        assertEquals(size(categoryEntities), 1);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_03_createCategoryObject() {
        Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();
        int cos = size(coEntityRepository.findAll());

        assertEquals(cos, 0);

        taxonomyService.createCategoryObject(categoryEntities.iterator().next().getCategoryId(), TEST_CATEGORY_OBJECT_NAME,
                TEST_CATEGORY_OBJECT_DESC);

        Iterable<CategoryObjectEntity> coEntites = coEntityRepository.findAll();

        assertEquals(size(coEntites), 1);

        CategoryObjectEntity coEntity = coEntites.iterator().next();

        assertEquals(coEntity.getName(), TEST_CATEGORY_OBJECT_NAME);
        assertEquals(coEntity.getDescription(), TEST_CATEGORY_OBJECT_DESC);
        assertTrue(coEntity.getCategory() != null);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_04_editTaxonomy() {
        Iterable<TaxonomyEntity> taxonomyEntities = taxonomyEntityRepository.findAll();
        TaxonomyEntity taxonomyEntity = taxonomyEntities.iterator().next();

        assertEquals(taxonomyEntity.getName(), TEST_TAXONOMY);

        taxonomyService.editTaxonomy(taxonomyEntity.getTaxonomyId(), "NewTaxonomyName");

        taxonomyEntities = taxonomyEntityRepository.findAll();

        taxonomyEntity = taxonomyEntities.iterator().next();

        assertEquals(taxonomyEntity.getName(), "NewTaxonomyName");

        taxonomyService.editTaxonomy(taxonomyEntity.getTaxonomyId(), TEST_TAXONOMY);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_05_editCategory() {
        Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();
        CategoryEntity categoryEntity = categoryEntities.iterator().next();

        assertEquals(categoryEntity.getName(), TEST_CATEGORY_NAME);

        taxonomyService.editCategory(categoryEntity.getCategoryId(), "NewCategoryName");

        categoryEntities = categoryEntityRepository.findAll();

        categoryEntity = categoryEntities.iterator().next();

        assertEquals(categoryEntity.getName(), "NewCategoryName");

        taxonomyService.editCategory(categoryEntity.getCategoryId(), TEST_CATEGORY_NAME);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_06_editCategoryObject() {
        Iterable<CategoryObjectEntity> categoryObjectEntities = coEntityRepository.findAll();
        CategoryObjectEntity categoryObjectEntity = categoryObjectEntities.iterator().next();

        assertEquals(categoryObjectEntity.getName(), TEST_CATEGORY_OBJECT_NAME);
        assertEquals(categoryObjectEntity.getDescription(), TEST_CATEGORY_OBJECT_DESC);

        taxonomyService.editCategoryObject(categoryObjectEntity.getCategoryObjectId(), "NewCategoryObjectName",
                "NewCategoryObjectDesc");

        categoryObjectEntities = coEntityRepository.findAll();

        categoryObjectEntity = categoryObjectEntities.iterator().next();

        assertEquals(categoryObjectEntity.getName(), "NewCategoryObjectName");
        assertEquals(categoryObjectEntity.getDescription(), "NewCategoryObjectDesc");

        taxonomyService.editCategoryObject(categoryObjectEntity.getCategoryObjectId(), TEST_CATEGORY_OBJECT_NAME,
                TEST_CATEGORY_OBJECT_DESC);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_07_deleteCategoryObject() {
        Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();
        CategoryEntity categoryEntity = categoryEntities.iterator().next();

        CategoryObjectEntity categoryObjectEntity = new CategoryObjectEntity();
        categoryObjectEntity.setName(TO_BE_DELETED);
        categoryObjectEntity.setDescription(TO_BE_DELETED);
        categoryObjectEntity.setCategory(categoryEntity);

        categoryObjectEntity = coEntityRepository.save(categoryObjectEntity);

        Iterable<CategoryObjectEntity> categoryObjectEntities = coEntityRepository.findAll();

        int categoryObjects = size(categoryObjectEntities);
        assertEquals(categoryObjects, 2);

        taxonomyService.deleteCategoryObject(categoryObjectEntity.getCategoryObjectId());

        categoryObjectEntities = coEntityRepository.findAll();

        categoryObjects = size(categoryObjectEntities);
        assertEquals(categoryObjects, 1);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_08_deleteCategory() {
        Iterable<TaxonomyEntity> taxonomyEntities = taxonomyEntityRepository.findAll();
        TaxonomyEntity taxonomyEntity = taxonomyEntities.iterator().next();

        CategoryEntity categoryEntity = new CategoryEntity();
        categoryEntity.setName(TO_BE_DELETED);
        categoryEntity.setTaxonomy(taxonomyEntity);

        categoryEntity = categoryEntityRepository.save(categoryEntity);

        Iterable<CategoryEntity> categoryEntities = categoryEntityRepository.findAll();

        int categories = size(categoryEntities);
        assertEquals(categories, 2);

        taxonomyService.deleteCategory(categoryEntity.getCategoryId());

        categoryEntities = categoryEntityRepository.findAll();

        categories = size(categoryEntities);
        assertEquals(categories, 1);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void taxonomyService_09_deleteTaxonomy() {
        TaxonomyEntity taxonomyEntity = new TaxonomyEntity();
        taxonomyEntity.setName(TO_BE_DELETED);

        taxonomyEntity = taxonomyEntityRepository.save(taxonomyEntity);

        Iterable<TaxonomyEntity> taxonomyEntities = taxonomyEntityRepository.findAll();

        int taxonomies = size(taxonomyEntities);
        assertEquals(taxonomies, 2);

        taxonomyService.deleteTaxonomy(taxonomyEntity.getTaxonomyId());

        taxonomyEntities = taxonomyEntityRepository.findAll();

        taxonomies = size(taxonomyEntities);
        assertEquals(taxonomies, 1);
    }

}
