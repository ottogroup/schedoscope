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
package org.schedoscope.metascope.controller;

import javax.servlet.http.HttpServletRequest;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.TaxonomyEntity;
import org.schedoscope.metascope.service.MetadataEntityService;
import org.schedoscope.metascope.service.TaxonomyService;
import org.schedoscope.metascope.service.UserEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class TaxonomyController {

	@Autowired
	private TaxonomyService taxonomyService;

	@Autowired
	private UserEntityService userEntityService;

	@Autowired
	private MetadataEntityService metadataEntityService;

	@Autowired
	private MetascopeConfig config;

	@RequestMapping(value = "/admin/taxonomies", method = RequestMethod.GET)
	public ModelAndView createTaxonomy(HttpServletRequest request) {
		ModelAndView mav = new ModelAndView("body/admin/taxonomy/taxonomy");

		boolean admin = userEntityService.isAdmin();
		boolean withUserManagement = config.withUserManagement();
		String schedoscopeTimestamp = metadataEntityService
				.getMetadataValue("timestamp");
		Iterable<TaxonomyEntity> taxonomies = taxonomyService.getTaxonomies();

		mav.addObject("schedoscopeTimestamp", schedoscopeTimestamp);
		mav.addObject("userEntityService", userEntityService);
		mav.addObject("admin", admin);
		mav.addObject("userMgmnt", withUserManagement);
		mav.addObject("taxonomies", taxonomies);

		return mav;
	}

	@RequestMapping(value = "/admin/taxonomy/create", method = RequestMethod.POST)
	public String createTaxonomy(HttpServletRequest request, String taxonomyName) {
		long taxonomyId = this.taxonomyService.createTaxonomy(taxonomyName);
		return "redirect:" + request.getHeader("Referer") + "#taxonomy"
				+ taxonomyId;
	}

	@RequestMapping(value = "/admin/taxonomy/edit", method = RequestMethod.POST)
	public String editTaxonomy(HttpServletRequest request, Long taxonomyId,
			String taxonomyName) {
		this.taxonomyService.editTaxonomy(taxonomyId, taxonomyName);
		return "redirect:" + request.getHeader("Referer") + "#taxonomy"
				+ taxonomyId;
	}

	@RequestMapping(value = "/admin/taxonomy/delete", method = RequestMethod.POST)
	public String deleteTaxonomy(HttpServletRequest request, Long taxonomyId) {
		this.taxonomyService.deleteTaxonomy(taxonomyId);
		return "redirect:" + request.getHeader("Referer");
	}

	@RequestMapping(value = "/admin/category/create", method = RequestMethod.POST)
	public String createCategory(HttpServletRequest request, Long taxonomyId,
			String categoryName) {
		long categoryId = this.taxonomyService.createCategory(taxonomyId,
				categoryName);
		return "redirect:" + request.getHeader("Referer") + "#categoryObjects"
				+ categoryId;
	}

	@RequestMapping(value = "/admin/category/edit", method = RequestMethod.POST)
	public String editCategory(HttpServletRequest request, Long categoryId,
			String categoryName) {
		this.taxonomyService.editCategory(categoryId, categoryName);
		return "redirect:" + request.getHeader("Referer") + "#categoryObjects"
				+ categoryId;
	}

	@RequestMapping(value = "/admin/category/delete", method = RequestMethod.POST)
	public String deleteCategory(HttpServletRequest request, Long categoryId) {
		this.taxonomyService.deleteCategory(categoryId);
		return "redirect:" + request.getHeader("Referer") + "#categoryObjects"
				+ categoryId;
	}

	@RequestMapping(value = "/admin/categoryobject/create", method = RequestMethod.POST)
	public String createCategoryObject(HttpServletRequest request,
			Long categoryId, String categoryObjectName, String description) {
		this.taxonomyService.createCategoryObject(categoryId,
				categoryObjectName, description);
		return "redirect:" + request.getHeader("Referer") + "#categoryObjects"
				+ categoryId;
	}

	@RequestMapping(value = "/admin/categoryobject/edit", method = RequestMethod.POST)
	public String editCategoryObject(HttpServletRequest request,
			Long categoryObjectId, String categoryObjectName, String description) {
		long categoryId = this.taxonomyService.editCategoryObject(
				categoryObjectId, categoryObjectName, description);
		return "redirect:" + request.getHeader("Referer") + "#categoryObjects"
				+ categoryId;
	}

	@RequestMapping(value = "/admin/categoryobject/delete", method = RequestMethod.POST)
	public String deleteCategoryObject(HttpServletRequest request,
			Long categoryObjectId) {
		long categoryId = this.taxonomyService
				.deleteCategoryObject(categoryObjectId);
		return "redirect:" + request.getHeader("Referer") + "#categoryObjects"
				+ categoryId;
	}

}
