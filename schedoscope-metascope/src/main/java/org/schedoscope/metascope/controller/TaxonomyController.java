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

import org.schedoscope.metascope.service.TaxonomyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class TaxonomyController {

  @Autowired
  private TaxonomyService taxonomyService;

  @RequestMapping(value = "/admin/category/create", method = RequestMethod.POST)
  public String createCategory(HttpServletRequest request, String category) {
    this.taxonomyService.createCategory(category);
    return "redirect:" + request.getHeader("Referer");
  }

  @RequestMapping(value = "/admin/category/delete", method = RequestMethod.POST)
  public String deleteCategory(HttpServletRequest request, String category) {
    this.taxonomyService.deleteCategory(category);
    return "redirect:" + request.getHeader("Referer");
  }

  @RequestMapping(value = "/admin/businessobject/create", method = RequestMethod.POST)
  public String createBusinessObject(HttpServletRequest request, String category, String bo, String description) {
    this.taxonomyService.createBusinessObject(category, bo, description);
    return "redirect:" + request.getHeader("Referer");
  }

  @RequestMapping(value = "/admin/businessobject/edit", method = RequestMethod.POST)
  public String editBusinessObject(HttpServletRequest request, String category, String bo, String oldCategory,
      String oldBo, String description) {
    this.taxonomyService.editBusinessObject(category, bo, oldCategory, oldBo, description);
    return "redirect:" + request.getHeader("Referer");
  }

  @RequestMapping(value = "/admin/businessobject/delete", method = RequestMethod.POST)
  public String deleteBusinessObject(HttpServletRequest request, String category, String businessobject) {
    this.taxonomyService.deleteBusinessObject(category, businessobject);
    return "redirect:" + request.getHeader("Referer");
  }

}
