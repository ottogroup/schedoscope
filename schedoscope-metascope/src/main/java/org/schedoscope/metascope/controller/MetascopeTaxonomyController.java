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
package org.schedoscope.metascope.controller;

import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.model.MetascopeTaxonomy;
import org.schedoscope.metascope.service.MetascopeMetadataService;
import org.schedoscope.metascope.service.MetascopeTaxonomyService;
import org.schedoscope.metascope.service.MetascopeUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;

@Controller
public class MetascopeTaxonomyController {

  @Autowired
  private MetascopeTaxonomyService metascopeTaxonomyService;

  @Autowired
  private MetascopeUserService metascopeUserService;

  @Autowired
  private MetascopeMetadataService metascopeMetadataService;

  @Autowired
  private MetascopeConfig config;

  @RequestMapping(value = "/admin/taxonomies", method = RequestMethod.GET)
  public ModelAndView createTaxonomy(HttpServletRequest request) {
    ModelAndView mav = new ModelAndView("body/admin/taxonomy/taxonomy");

    boolean admin = metascopeUserService.isAdmin();
    boolean withUserManagement = config.withUserManagement();
    String schedoscopeTimestamp = metascopeMetadataService.getMetadataValue("timestamp");
    Iterable<MetascopeTaxonomy> taxonomies = metascopeTaxonomyService.getTaxonomies();

    mav.addObject("schedoscopeTimestamp", schedoscopeTimestamp);
    mav.addObject("userEntityService", metascopeUserService);
    mav.addObject("admin", admin);
    mav.addObject("userMgmnt", withUserManagement);
    mav.addObject("taxonomies", taxonomies);

    return mav;
  }

  @RequestMapping(value = "/admin/taxonomy/create", method = RequestMethod.POST)
  public String createTaxonomy(HttpServletRequest request, String taxonomyName) {
    long taxonomyId = this.metascopeTaxonomyService.createTaxonomy(taxonomyName);
    return "redirect:" + request.getHeader("Referer") + "#taxonomy" + taxonomyId;
  }

  @RequestMapping(value = "/admin/taxonomy/edit", method = RequestMethod.POST)
  public String editTaxonomy(HttpServletRequest request, Long taxonomyId, String taxonomyName) {
    this.metascopeTaxonomyService.editTaxonomy(taxonomyId, taxonomyName);
    return "redirect:" + request.getHeader("Referer") + "#taxonomy" + taxonomyId;
  }

  @RequestMapping(value = "/admin/taxonomy/delete", method = RequestMethod.POST)
  public String deleteTaxonomy(HttpServletRequest request, Long taxonomyId) {
    this.metascopeTaxonomyService.deleteTaxonomy(taxonomyId);
    return "redirect:" + request.getHeader("Referer");
  }

  @RequestMapping(value = "/admin/category/create", method = RequestMethod.POST)
  public String createCategory(HttpServletRequest request, Long taxonomyId, String categoryName) {
    long categoryId = this.metascopeTaxonomyService.createCategory(taxonomyId, categoryName);
    return "redirect:" + request.getHeader("Referer") + "#categoryObjects" + categoryId;
  }

  @RequestMapping(value = "/admin/category/edit", method = RequestMethod.POST)
  public String editCategory(HttpServletRequest request, Long categoryId, String categoryName) {
    this.metascopeTaxonomyService.editCategory(categoryId, categoryName);
    return "redirect:" + request.getHeader("Referer") + "#categoryObjects" + categoryId;
  }

  @RequestMapping(value = "/admin/category/delete", method = RequestMethod.POST)
  public String deleteCategory(HttpServletRequest request, Long categoryId) {
    this.metascopeTaxonomyService.deleteCategory(categoryId);
    return "redirect:" + request.getHeader("Referer") + "#categoryObjects" + categoryId;
  }

  @RequestMapping(value = "/admin/categoryobject/create", method = RequestMethod.POST)
  public String createCategoryObject(HttpServletRequest request, Long categoryId, String categoryObjectName,
                                     String description) {
    this.metascopeTaxonomyService.createCategoryObject(categoryId, categoryObjectName, description);
    return "redirect:" + request.getHeader("Referer") + "#categoryObjects" + categoryId;
  }

  @RequestMapping(value = "/admin/categoryobject/edit", method = RequestMethod.POST)
  public String editCategoryObject(HttpServletRequest request, Long categoryObjectId, String categoryObjectName,
                                   String description) {
    long categoryId = this.metascopeTaxonomyService.editCategoryObject(categoryObjectId, categoryObjectName, description);
    return "redirect:" + request.getHeader("Referer") + "#categoryObjects" + categoryId;
  }

  @RequestMapping(value = "/admin/categoryobject/delete", method = RequestMethod.POST)
  public String deleteCategoryObject(HttpServletRequest request, Long categoryObjectId) {
    long categoryId = this.metascopeTaxonomyService.deleteCategoryObject(categoryObjectId);
    return "redirect:" + request.getHeader("Referer") + "#categoryObjects" + categoryId;
  }

}
