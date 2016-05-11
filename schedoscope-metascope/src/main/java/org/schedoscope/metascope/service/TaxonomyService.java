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

import java.util.List;

import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.CategoryEntity;
import org.schedoscope.metascope.model.CategoryObjectEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.TaxonomyEntity;
import org.schedoscope.metascope.repository.CategoryEntityRepository;
import org.schedoscope.metascope.repository.CategoryObjectEntityRepository;
import org.schedoscope.metascope.repository.TableEntityRepository;
import org.schedoscope.metascope.repository.TaxonomyEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TaxonomyService {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyService.class);
  
	@Autowired
	private UserEntityService userEntityService;

	@Autowired
	private TaxonomyEntityRepository taxonomyEntityRepository;
	
	@Autowired
	private CategoryEntityRepository categoryEntityRepository;
	
	@Autowired
	private CategoryObjectEntityRepository categoryObjectEntityRepository;
	
	@Autowired
	private TableEntityRepository tableEntityRepository;
	
	@Autowired
	private SolrFacade solr;

	// 
	// ### Taxonomy ###
	//
	
	public Iterable<TaxonomyEntity> getTaxonomies() {
		return taxonomyEntityRepository.findAll();
  }
	
	@Transactional
	public long createTaxonomy(String taxonomyName) {
	  if (taxonomyName == null) {
	  	LOG.info("Taxonomy name can't be null");
	  	return -1;
	  }
	  
		if (taxonomyEntityRepository.findByName(taxonomyName) != null) {
	  	LOG.info("Taxonomy already exists.");
	  	return -1;
	  }
	  
	  TaxonomyEntity taxonomyEntity = new TaxonomyEntity();
	  taxonomyEntity.setName(taxonomyName);
	  long taxonomyId = taxonomyEntityRepository.save(taxonomyEntity).getTaxonomyId();
	  LOG.info("User '{}' created new taxonomy '{}'", userEntityService.getUser().getUsername(), taxonomyName);
	  
	  return taxonomyId;
  }
	
	@Transactional
	public void editTaxonomy(Long taxonomyId, String taxonomyName) {
	  if (taxonomyId == null) {
	  	LOG.info("Taxonomy ID can't be null");
	  	return;
	  }
	  
	  if (taxonomyName == null) {
	  	LOG.info("Taxonomy name can't be null");
	  	return;
	  }
	  
		TaxonomyEntity taxonomyEntity = taxonomyEntityRepository.findOne(taxonomyId);
	  if (taxonomyEntity == null) {
	  	LOG.info("Taxonomy does not exists.");
	  	return;
	  }
	  
	  String oldName = taxonomyEntity.getName();
	  
		taxonomyEntity.setName(taxonomyName);
	  taxonomyEntityRepository.save(taxonomyEntity);
	  LOG.info("User '{}' changed taxonomy name from '{}' to '{}'", userEntityService.getUser().getUsername(), 
	  		oldName, taxonomyName);
  }
	
	@Transactional
	public void deleteTaxonomy(Long taxonomyId) {
	  if (taxonomyId == null) {
	  	LOG.info("Taxonomy ID can't be null");
	  	return;
	  }
	  
		TaxonomyEntity taxonomyEntity = taxonomyEntityRepository.findOne(taxonomyId);
	  if (taxonomyEntity == null) {
	  	LOG.info("Taxonomy does not exists.");
	  	return;
	  }
	  
	  deleteCategoryObjectsFromTable(taxonomyEntity);
	  
	  taxonomyEntityRepository.delete(taxonomyEntity);
	  LOG.info("User '{}' deleted taxonomy '{}'", userEntityService.getUser().getUsername(), taxonomyEntity.getName());
  }
	
	// 
	// ### Category ###
	//
	
	@Transactional
	public long createCategory(Long taxonomyId, String categoryName) {
	  if (taxonomyId == null) {
	  	LOG.info("Taxonomy ID can't be null");
	  	return -1;
	  }
	  
	  if (categoryName == null) {
	  	LOG.info("Category name can't be null");
	  	return -1;
	  }
	  
		TaxonomyEntity taxonomyEntity = taxonomyEntityRepository.findOne(taxonomyId);
	  if (taxonomyEntity == null) {
	  	LOG.info("Taxonomy does not exists.");
	  	return -1;
	  }
	  
	  for (CategoryEntity categoryEntity : taxonomyEntity.getCategories()) {
	    if (categoryEntity.getName().equals(categoryName)) {
	    	LOG.info("Category already exists.");
	    	return -1;
	    }
    }
	  
	  CategoryEntity categoryEntity = new CategoryEntity();
	  categoryEntity.setName(categoryName);
	  categoryEntity.setTaxonomy(taxonomyEntity);
	  long categoryId = categoryEntityRepository.save(categoryEntity).getCategoryId();
	  
	  taxonomyEntity.getCategories().add(categoryEntity);
	  taxonomyEntityRepository.save(taxonomyEntity);
	  LOG.info("User '{}' created new category '{}'", userEntityService.getUser().getUsername(), categoryName);
	  return categoryId;
  }
	
	@Transactional
	public void editCategory(Long categoryId, String categoryName) {
	  if (categoryId == null) {
	  	LOG.info("Category ID can't be null");
	  	return;
	  }
	  
	  if (categoryName == null) {
	  	LOG.info("Category name can't be null");
	  	return;
	  }
	  
	  CategoryEntity categoryEntity = categoryEntityRepository.findOne(categoryId);
	  if (categoryEntity == null) {
	  	LOG.info("Category does not exists.");
    	return;
	  }
	  
	  String oldName = categoryEntity.getName();
	  
	  categoryEntity.setName(categoryName);
	  categoryEntityRepository.save(categoryEntity);
	  LOG.info("User '{}' changed category name from '{}' to '{}'", userEntityService.getUser().getUsername(), 
	  		oldName, categoryName);
  }
	
	@Transactional
	public void deleteCategory(Long categoryId) {
	  if (categoryId == null) {
	  	LOG.info("Category ID can't be null");
	  	return;
	  }
	  
	  CategoryEntity categoryEntity = categoryEntityRepository.findOne(categoryId);
	  if (categoryEntity == null) {
	  	LOG.info("Category does not exists.");
    	return;
	  }
	  
	  deleteCategoryObjectsFromTable(categoryEntity);
	  
	  TaxonomyEntity taxonomyEntity = categoryEntity.getTaxonomy();
	  taxonomyEntity.getCategories().remove(categoryEntity);
	  
	  categoryEntityRepository.delete(categoryEntity);
	  taxonomyEntityRepository.save(taxonomyEntity);
	  LOG.info("User '{}' deleted category '{}'", userEntityService.getUser().getUsername(), categoryEntity.getName());
  }
	
	// 
	// ### Category Object ###
	//

	@Transactional
	public void createCategoryObject(Long categoryId, String categoryObjectName, String description) {
	  if (categoryId == null) {
	  	LOG.info("Category ID can't be null");
	  	return;
	  }
	  
	  if (categoryObjectName == null) {
	  	LOG.info("Category Object name can't be null");
	  	return;
	  }
	  
	  if (description == null) {
	  	LOG.info("Category Object description can't be null");
	  	return;
	  }
	  
		CategoryEntity categoryEntity = categoryEntityRepository.findOne(categoryId);
	  if (categoryEntity == null) {
	  	LOG.info("Category does not exists.");
	  	return;
	  }
	  
	  for (CategoryObjectEntity categoryObjectEntity : categoryEntity.getCategoryObjects()) {
	    if (categoryObjectEntity.getName().equals(categoryObjectName)) {
	    	LOG.info("Category Object with name '{}' already exists.", categoryObjectName);
	    	return;
	    }
    }
	  
	  CategoryObjectEntity categoryObjectEntity = new CategoryObjectEntity();
	  categoryObjectEntity.setName(categoryObjectName);
	  categoryObjectEntity.setDescription(description);
	  categoryObjectEntity.setCategory(categoryEntity);
	  categoryObjectEntityRepository.save(categoryObjectEntity);
	  
	  categoryEntity.getCategoryObjects().add(categoryObjectEntity);
	  categoryEntityRepository.save(categoryEntity);
	  
	  LOG.info("User '{}' created new category object '{}'", userEntityService.getUser().getUsername(), categoryObjectName);
  }
	
	@Transactional
	public long editCategoryObject(Long categoryObjectId, String categoryObjectName, String description) {
	  if (categoryObjectId == null) {
	  	LOG.info("Category ID can't be null");
	  	return -1;
	  }

		CategoryObjectEntity categoryObjectEntity = categoryObjectEntityRepository.findOne(categoryObjectId);
	  if (categoryObjectEntity == null) {
	  	LOG.info("Category Object does not exists.");
	  	return -1;
	  }
	  
	  String oldName = categoryObjectEntity.getName();
	  
	  if (categoryObjectName != null) {
		  categoryObjectEntity.setName(categoryObjectName);
	  }

	  if (description != null) {
		  categoryObjectEntity.setDescription(description);
	  }

	  categoryObjectEntityRepository.save(categoryObjectEntity);
	  LOG.info("User '{}' changed category name from '{}' to '{}' with description '{}'", userEntityService.getUser().getUsername(), 
	  		oldName, categoryObjectName, description);
	  return categoryObjectEntity.getCategory().getCategoryId();
  }
	
	@Transactional
	public long deleteCategoryObject(Long categoryObjectId) {
	  if (categoryObjectId == null) {
	  	LOG.info("Category ID can't be null");
	  	return -1;
	  }
	  
		CategoryObjectEntity categoryObjectEntity = categoryObjectEntityRepository.findOne(categoryObjectId);
	  if (categoryObjectEntity == null) {
	  	LOG.info("Category Object does not exists.");
	  	return -1;
	  }
	  
	  deleteCategoryObjectsFromTable(categoryObjectEntity);
	  
	  CategoryEntity categoryEntity = categoryObjectEntity.getCategory();
	  categoryEntity.getCategoryObjects().remove(categoryObjectEntity);
	  
	  categoryObjectEntityRepository.delete(categoryObjectEntity);
	  categoryEntityRepository.save(categoryEntity);
	  LOG.info("User '{}' deleted category object '{}'", userEntityService.getUser().getUsername(), categoryObjectEntity.getName());
	  return categoryEntity.getCategoryId();
  }
	
	private void deleteCategoryObjectsFromTable(TaxonomyEntity taxonomyEntity) {
		for (CategoryEntity categoryEntity : taxonomyEntity.getCategories()) {
			deleteCategoryObjectsFromTable(categoryEntity);
    } 
	}
	
	private void deleteCategoryObjectsFromTable(CategoryEntity categoryEntity) {
		for (CategoryObjectEntity categoryObjectEntity : categoryEntity.getCategoryObjects()) {
	    deleteCategoryObjectsFromTable(categoryObjectEntity);
    }
	}
	
	private void deleteCategoryObjectsFromTable(CategoryObjectEntity categoryObjectEntity) {
		List<TableEntity> tableEntities = tableEntityRepository.findByCategoryObject(categoryObjectEntity);
	  
	  for (TableEntity tableEntity : tableEntities) {
      tableEntity.removeCategoryObjectById(categoryObjectEntity.getCategoryObjectId());
	  	tableEntityRepository.save(tableEntity);
	  	solr.updateTableEntityAsync(tableEntity, true);
    }
	}
	
}
