/**
 * Copyright 2017 Otto (GmbH & Co KG)
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

import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.MetascopeCategory;
import org.schedoscope.metascope.model.MetascopeCategoryObject;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeTaxonomy;
import org.schedoscope.metascope.repository.MetascopeCategoryObjectRepository;
import org.schedoscope.metascope.repository.MetascopeCategoryRepository;
import org.schedoscope.metascope.repository.MetascopeTableRepository;
import org.schedoscope.metascope.repository.MetascopeTaxonomyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class MetascopeTaxonomyService {

  private static final Logger LOG = LoggerFactory.getLogger(MetascopeTaxonomyService.class);

  @Autowired
  private MetascopeUserService metascopeUserService;

  @Autowired
  private MetascopeTaxonomyRepository metascopeTaxonomyRepository;

  @Autowired
  private MetascopeCategoryRepository metascopeCategoryRepository;

  @Autowired
  private MetascopeCategoryObjectRepository metascopeCategoryObjectRepository;

  @Autowired
  private MetascopeTableRepository metascopeTableRepository;

  @Autowired
  private SolrFacade solr;

  //
  // ### Taxonomy ###
  //

  public Iterable<MetascopeTaxonomy> getTaxonomies() {
    return metascopeTaxonomyRepository.findAll();
  }

  @Transactional
  public long createTaxonomy(String taxonomyName) {
    if (taxonomyName == null) {
      LOG.info("Taxonomy name can't be null");
      return -1;
    }

    if (metascopeTaxonomyRepository.findByName(taxonomyName) != null) {
      LOG.info("Taxonomy already exists.");
      return -1;
    }

    MetascopeTaxonomy metascopeTaxonomy = new MetascopeTaxonomy();
    metascopeTaxonomy.setName(taxonomyName);
    long taxonomyId = metascopeTaxonomyRepository.save(metascopeTaxonomy).getTaxonomyId();
    LOG.info("User '{}' created new taxonomy '{}'", metascopeUserService.getUser().getUsername(), taxonomyName);

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

    MetascopeTaxonomy metascopeTaxonomy = metascopeTaxonomyRepository.findOne(taxonomyId);
    if (metascopeTaxonomy == null) {
      LOG.info("Taxonomy does not exists.");
      return;
    }

    String oldName = metascopeTaxonomy.getName();

    metascopeTaxonomy.setName(taxonomyName);
    metascopeTaxonomyRepository.save(metascopeTaxonomy);
    LOG.info("User '{}' changed taxonomy name from '{}' to '{}'", metascopeUserService.getUser().getUsername(), oldName,
      taxonomyName);
  }

  @Transactional
  public void deleteTaxonomy(Long taxonomyId) {
    if (taxonomyId == null) {
      LOG.info("Taxonomy ID can't be null");
      return;
    }

    MetascopeTaxonomy metascopeTaxonomy = metascopeTaxonomyRepository.findOne(taxonomyId);
    if (metascopeTaxonomy == null) {
      LOG.info("Taxonomy does not exists.");
      return;
    }

    deleteCategoryObjectsFromTable(metascopeTaxonomy);

    metascopeTaxonomyRepository.delete(metascopeTaxonomy);
    LOG.info("User '{}' deleted taxonomy '{}'", metascopeUserService.getUser().getUsername(), metascopeTaxonomy.getName());
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

    MetascopeTaxonomy metascopeTaxonomy = metascopeTaxonomyRepository.findOne(taxonomyId);
    if (metascopeTaxonomy == null) {
      LOG.info("Taxonomy does not exists.");
      return -1;
    }

    for (MetascopeCategory MetascopeCategory : metascopeTaxonomy.getCategories()) {
      if (MetascopeCategory.getName().equals(categoryName)) {
        LOG.info("Category already exists.");
        return -1;
      }
    }

    MetascopeCategory MetascopeCategory = new MetascopeCategory();
    MetascopeCategory.setName(categoryName);
    MetascopeCategory.setTaxonomy(metascopeTaxonomy);
    long categoryId = metascopeCategoryRepository.save(MetascopeCategory).getCategoryId();

    metascopeTaxonomy.getCategories().add(MetascopeCategory);
    metascopeTaxonomyRepository.save(metascopeTaxonomy);
    LOG.info("User '{}' created new category '{}'", metascopeUserService.getUser().getUsername(), categoryName);
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

    MetascopeCategory MetascopeCategory = metascopeCategoryRepository.findOne(categoryId);
    if (MetascopeCategory == null) {
      LOG.info("Category does not exists.");
      return;
    }

    String oldName = MetascopeCategory.getName();

    MetascopeCategory.setName(categoryName);
    metascopeCategoryRepository.save(MetascopeCategory);
    LOG.info("User '{}' changed category name from '{}' to '{}'", metascopeUserService.getUser().getUsername(), oldName,
      categoryName);
  }

  @Transactional
  public void deleteCategory(Long categoryId) {
    if (categoryId == null) {
      LOG.info("Category ID can't be null");
      return;
    }

    MetascopeCategory MetascopeCategory = metascopeCategoryRepository.findOne(categoryId);
    if (MetascopeCategory == null) {
      LOG.info("Category does not exists.");
      return;
    }

    deleteCategoryObjectsFromTable(MetascopeCategory);

    MetascopeTaxonomy metascopeTaxonomy = MetascopeCategory.getTaxonomy();
    metascopeTaxonomy.getCategories().remove(MetascopeCategory);

    metascopeCategoryRepository.delete(MetascopeCategory);
    metascopeTaxonomyRepository.save(metascopeTaxonomy);
    LOG.info("User '{}' deleted category '{}'", metascopeUserService.getUser().getUsername(), MetascopeCategory.getName());
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

    MetascopeCategory MetascopeCategory = metascopeCategoryRepository.findOne(categoryId);
    if (MetascopeCategory == null) {
      LOG.info("Category does not exists.");
      return;
    }

    for (MetascopeCategoryObject MetascopeCategoryObject : MetascopeCategory.getCategoryObjects()) {
      if (MetascopeCategoryObject.getName().equals(categoryObjectName)) {
        LOG.info("Category Object with name '{}' already exists.", categoryObjectName);
        return;
      }
    }

    MetascopeCategoryObject metascopeCategoryObject = new MetascopeCategoryObject();
    metascopeCategoryObject.setName(categoryObjectName);
    metascopeCategoryObject.setDescription(description);
    metascopeCategoryObject.setCategory(MetascopeCategory);
    metascopeCategoryObjectRepository.save(metascopeCategoryObject);

    MetascopeCategory.getCategoryObjects().add(metascopeCategoryObject);
    metascopeCategoryRepository.save(MetascopeCategory);

    LOG.info("User '{}' created new category object '{}'", metascopeUserService.getUser().getUsername(),
      categoryObjectName);
  }

  @Transactional
  public long editCategoryObject(Long categoryObjectId, String categoryObjectName, String description) {
    if (categoryObjectId == null) {
      LOG.info("Category ID can't be null");
      return -1;
    }

    MetascopeCategoryObject MetascopeCategoryObject = metascopeCategoryObjectRepository.findOne(categoryObjectId);
    if (MetascopeCategoryObject == null) {
      LOG.info("Category Object does not exists.");
      return -1;
    }

    String oldName = MetascopeCategoryObject.getName();

    if (categoryObjectName != null) {
      MetascopeCategoryObject.setName(categoryObjectName);
    }

    if (description != null) {
      MetascopeCategoryObject.setDescription(description);
    }

    metascopeCategoryObjectRepository.save(MetascopeCategoryObject);
    LOG.info("User '{}' changed category name from '{}' to '{}' with description '{}'", metascopeUserService.getUser()
      .getUsername(), oldName, categoryObjectName, description);
    return MetascopeCategoryObject.getCategory().getCategoryId();
  }

  @Transactional
  public long deleteCategoryObject(Long categoryObjectId) {
    if (categoryObjectId == null) {
      LOG.info("Category ID can't be null");
      return -1;
    }

    MetascopeCategoryObject MetascopeCategoryObject = metascopeCategoryObjectRepository.findOne(categoryObjectId);
    if (MetascopeCategoryObject == null) {
      LOG.info("Category Object does not exists.");
      return -1;
    }

    deleteCategoryObjectsFromTable(MetascopeCategoryObject);

    MetascopeCategory MetascopeCategory = MetascopeCategoryObject.getCategory();
    MetascopeCategory.getCategoryObjects().remove(MetascopeCategoryObject);

    metascopeCategoryObjectRepository.delete(MetascopeCategoryObject);
    metascopeCategoryRepository.save(MetascopeCategory);
    LOG.info("User '{}' deleted category object '{}'", metascopeUserService.getUser().getUsername(),
      MetascopeCategoryObject.getName());
    return MetascopeCategory.getCategoryId();
  }

  private void deleteCategoryObjectsFromTable(MetascopeTaxonomy metascopeTaxonomy) {
    for (MetascopeCategory MetascopeCategory : metascopeTaxonomy.getCategories()) {
      deleteCategoryObjectsFromTable(MetascopeCategory);
    }
  }

  private void deleteCategoryObjectsFromTable(MetascopeCategory MetascopeCategory) {
    for (MetascopeCategoryObject MetascopeCategoryObject : MetascopeCategory.getCategoryObjects()) {
      deleteCategoryObjectsFromTable(MetascopeCategoryObject);
    }
  }

  private void deleteCategoryObjectsFromTable(MetascopeCategoryObject MetascopeCategoryObject) {
    List<MetascopeTable> tables = metascopeTableRepository.findByCategoryObject(MetascopeCategoryObject);

    for (MetascopeTable table : tables) {
      table.removeCategoryObjectById(MetascopeCategoryObject.getCategoryObjectId());
      metascopeTableRepository.save(table);
      solr.updateTableEntityAsync(table, true);
    }
  }

}
