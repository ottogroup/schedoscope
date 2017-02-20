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
import org.schedoscope.metascope.model.*;
import org.schedoscope.metascope.service.*;
import org.schedoscope.metascope.util.HTMLUtil;
import org.schedoscope.metascope.util.ParseUtil;
import org.schedoscope.metascope.util.model.CategoryMap;
import org.schedoscope.metascope.util.model.HiveQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.support.RequestContextUtils;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Controller
public class MetascopeTableController {

  private static final Logger LOG = LoggerFactory.getLogger(MetascopeTableController.class);

  @Autowired
  private MetascopeTableService metascopeTableService;
  @Autowired
  private MetascopeViewService metascopeViewService;
  @Autowired
  private MetascopeUserService metascopeUserService;
  @Autowired
  private MetascopeTaxonomyService metascopeTaxonomyService;
  @Autowired
  private MetascopeDocumentationService metascopeDocumentationService;
  @Autowired
  private MetascopeDataDistributionService metascopeDataDistributionService;
  @Autowired
  private MetascopeStatusService metascopeStatusService;
  @Autowired
  private MetascopeConfig config;
  @Autowired
  private HTMLUtil htmlUtil;

  /**
   * Detail view of a table; path = '/table'
   *
   * @param request the HTTPServletRequest from the client
   * @return the table detail view
   */
  @RequestMapping(value = "/table", method = RequestMethod.GET)
  @Transactional
  public ModelAndView getTable(HttpServletRequest request) {
    ModelAndView mav = new ModelAndView("body/table/table");

    /* load table from repository */
    String fqdn = getParameter(request, "fqdn");
    MetascopeTable tableEntity = metascopeTableService.findByFqdn(fqdn);
    if (tableEntity == null) {
      return new ModelAndView(new RedirectView("/notfound"));
    }

    /* retrieve requested page for partitions section */
    String partitionPageParameter = getParameter(request, "partitionPage");
    int partitionPage = 1;
    if (partitionPageParameter != null) {
      Integer p = ParseUtil.tryParse(partitionPageParameter);
      if (p != null && p > 0) {
        partitionPage = p;
      }
    }

    /* get all taxonomies */
    Iterable<MetascopeTaxonomy> taxonomies = metascopeTaxonomyService.getTaxonomies();
    List<String> taxonomyNames = new ArrayList<String>();
    for (MetascopeTaxonomy taxonomyEntity : taxonomies) {
      taxonomyNames.add(taxonomyEntity.getName());
    }

    Map<String, CategoryMap> tableTaxonomies = metascopeTableService.getTableTaxonomies(tableEntity);

    /* get all users for user management and owner auto completion */
    Iterable<MetascopeUser> users = metascopeUserService.getAllUser();

    /* get all registered table owners for auto completion */
    Set<String> owner = metascopeTableService.getAllOwner();
    owner.remove(null);
    for (MetascopeUser user : users) {
      if (!owner.contains(user.getFullname())) {
        owner.add(user.getFullname());
      }
    }

    /* check data distribution calculation status */
    MetascopeDataDistributionService.Status dataDistStatus = metascopeDataDistributionService.checkStatus(tableEntity);
    if (dataDistStatus.equals(MetascopeDataDistributionService.Status.Finished)) {
      mav.addObject("ddMap", metascopeDataDistributionService.getDataDistribution(tableEntity));
    }

    /* check if this user is an admin */
    boolean isAdmin = metascopeUserService.isAdmin();

    /* check if this table is favourised */
    boolean isFavourite = metascopeUserService.isFavourite(tableEntity);

    boolean transitive = getParameter(request, "transitive") != null;
    if (transitive) {
      /* get transitive dependencies and successors */
      List<MetascopeTable> transitiveDependencies = metascopeTableService.getTransitiveDependencies(tableEntity);
      List<MetascopeTable> transitiveSuccessors = metascopeTableService.getTransitiveSuccessors(tableEntity);
      mav.addObject("transitiveDependencies", transitiveDependencies);
      mav.addObject("transitiveSuccessors", transitiveSuccessors);
    }

    /*
     * if table is partitioned, get the first parameter (see 'Data Distribution'
     * section)
     */
    Set<MetascopeField> parameters = tableEntity.getParameters();
    if (parameters.size() > 0) {
      mav.addObject("firstParam", parameters.iterator().next().getFieldName());
    }

    /* check if a draft autosave is available */
    boolean metascopeAutoSave = metascopeDocumentationService.checkForDraft(tableEntity, metascopeUserService.getUser());

    /* check if the responsible person has been changed */
    String personResponsible = getParameter(request, "personResponsible");

    /* check if the docuemntation has been changed */
    String documentation = getParameter(request, "documentation");

    /* check if a comment has been created/edited/deleted */
    String comment = getParameter(request, "comment");

    /* check if the taxonomy has been changed */
    String taxonomy = getParameter(request, "taxonomy");

    /*
     * local parameter indicates if the css and javascript should be containted
     * in the html. this can be useful when displaying views in third party
     * sites and apps
     */
    boolean local = getParameter(request, "local") != null;

    /* search result breadcrumb */
    String referer = request.getHeader("Referer");
    String searchResultBreadcrumb = null;
    if (referer != null) {
      String refererSplit[] = request.getHeader("Referer").split("\\?");
      if (refererSplit.length == 2 && refererSplit[0].contains("home") && hasSearchParameters(refererSplit[1])) {
        searchResultBreadcrumb = referer;
      }
    }

    /* make objects accessible for thymeleaf to render final view */
    mav.addObject("table", tableEntity);
    mav.addObject("userEntityService", metascopeUserService);
    mav.addObject("util", htmlUtil);
    mav.addObject("local", local);
    mav.addObject("partitionPage", partitionPage);
    mav.addObject("users", users);
    mav.addObject("owner", owner);
    mav.addObject("taxonomies", taxonomies);
    mav.addObject("taxonomyNames", taxonomyNames);
    mav.addObject("tableTaxonomies", tableTaxonomies);
    mav.addObject("admin", isAdmin);
    mav.addObject("draft", metascopeAutoSave);
    mav.addObject("isFavourite", isFavourite);
    mav.addObject("personResponsible", personResponsible);
    mav.addObject("documentation", documentation);
    mav.addObject("comment", comment);
    mav.addObject("taxonomy", taxonomy);
    mav.addObject("dataDistStatus", dataDistStatus.name().toLowerCase());
    mav.addObject("searchResultBreadcrumb", searchResultBreadcrumb);
    mav.addObject("userMgmnt", config.withUserManagement());

    return mav;
  }

  /**
   * Adds or removes a table from users favourites
   *
   * @param request the HTTPServletRequest from the client
   * @param fqdn    the table to add/remove from favourites
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/table/favourite", method = RequestMethod.POST)
  public String addOrRemoveFavourite(HttpServletRequest request, String fqdn) {
    metascopeTableService.addOrRemoveFavourite(fqdn);
    return "redirect:" + request.getHeader("Referer");
  }

  /**
   * Adds or removes the business objects from the table
   *
   * @param request         the HTTPServletRequest from the client
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/table/categoryobjects", method = RequestMethod.POST)
  public String addCategoryObject(HttpServletRequest request, RedirectAttributes redirAttr) {
    Map<String, String[]> parameterMap = request.getParameterMap();
    String[] fqdnArr = parameterMap.get("fqdn");
    String[] tagArr = parameterMap.get("tags");
    String fqdn = null;
    String tags = null;
    if (fqdnArr != null && fqdnArr.length > 0) {
      fqdn = fqdnArr[0];
    }
    if (tagArr != null && tagArr.length > 0) {
      tags = tagArr[0];
    }

    if (fqdn != null) {
      metascopeTableService.setCategoryObjects(fqdn, parameterMap);
      metascopeTableService.setTags(fqdn, tags);
    }
    redirAttr.addFlashAttribute("taxonomy", true);
    return "redirect:" + request.getHeader("Referer") + "#taxonomyContent";
  }

  /**
   * Changes the 'person responsible' of the table
   *
   * @param request the HTTPServletRequest from the client
   * @param fqdn    the table to be changed
   * @param person  the fullname of the new person responsible
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/table/owner", method = RequestMethod.POST)
  public String changeOwner(HttpServletRequest request, RedirectAttributes redirAttr, String fqdn, String person) {
    metascopeTableService.setPersonResponsible(fqdn, person);
    redirAttr.addFlashAttribute("personResponsible", person);
    return "redirect:" + request.getHeader("Referer");
  }

  /**
   * Changes the 'admin' properties of the table
   *
   * @param request                  request the HTTPServletRequest from the client
   * @param fqdn                     the table to be changed
   * @param dataTimestampField       the new timestamp field of the table
   * @param dataTimestampFieldFormat the corresponding format for the timestamp
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/admin/table", method = RequestMethod.POST)
  public String admin(HttpServletRequest request, String fqdn, String dataTimestampField, String dataTimestampFieldFormat) {
    metascopeTableService.setTimestampField(fqdn, dataTimestampField, dataTimestampFieldFormat);
    return "redirect:" + request.getHeader("Referer") + "#adminContent";
  }

  /**
   * Increases the view count by one
   *
   * @param fqdn the table
   */
  @RequestMapping(value = "/table/viewcount", method = RequestMethod.POST)
  @ResponseStatus(value = HttpStatus.OK)
  public void increaseViewCount(String fqdn) {
    metascopeTableService.increaseViewCount(fqdn);
  }

  /**
   * Returns a JSON for VisJS to render the lineage graph
   *
   * @param fqdn the table for which the lineage graph is requesed
   * @return JSON for VisJS
   */
  @RequestMapping(value = "/table/view/lineage", method = RequestMethod.GET)
  @ResponseBody
  public String getLineage(String fqdn) {
    MetascopeTable table = metascopeTableService.findByFqdn(fqdn);

    if (table == null) {
      return "table not found";
    }

    return metascopeTableService.getLineage(table);
  }

  /**
   * Returns the lineage detail when a node is selected in the lineage graph
   *
   * @param fqdn the table for which the lineage graph is requesed
   * @param type the type of the node (either transformation or table)
   * @return
   */
  @RequestMapping(value = "/table/view/lineage/detail", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getTableDetail(String fqdn, String type) {
    ModelAndView mav = null;
    MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
    if (table != null) {
      mav = new ModelAndView("body/table/sections/lineagedetail");
      mav.addObject("table", table);
      mav.addObject("type", type);
      mav.addObject("util", htmlUtil);
      mav.addObject("metascopeUserService", metascopeUserService);
    }
    return mav;
  }

  /**
   * Return a HTML table containing a data sample
   *
   * @param fqdn   the table for which the sample is requested
   * @param params partition parameter for filtering
   * @return
   */
  @RequestMapping(value = "/table/view/sample", method = RequestMethod.GET)
  @ResponseBody
  public String getSample(String fqdn, @RequestParam Map<String, String> params) {
    if (fqdn == null) {
      return null;
    }

    params.remove("fqdn");

    Future<HiveQueryResult> future = metascopeTableService.getSample(fqdn, params);
    HiveQueryResult sample;
    try {
      sample = future.get(10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      sample = new HiveQueryResult("Query timed out");
    } catch (Exception e) {
      LOG.warn("Could not execute query", e);
      sample = new HiveQueryResult("Internal server error, check logs");
    }
    return htmlUtil.convertQueryResultToTable(sample);
  }

  /**
   * Returns a HTML table for the partitions
   *
   * @param fqdn          the parent table of the partitions
   * @param partitionPage the requested partition page
   * @return a HTML table with sample data
   */
  @RequestMapping(value = "/table/view/views", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getViews(String fqdn, int partitionPage) {
    ModelAndView mav = new ModelAndView("body/table/sections/views");
    MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
    Page<MetascopeView> views = metascopeTableService.getRequestedViewPage(fqdn, new PageRequest(partitionPage - 1, 20));
    mav.addObject("table", table);
    mav.addObject("views", views);
    mav.addObject("util", htmlUtil);
    mav.addObject("statusService", metascopeStatusService);
    return mav;
  }

  /**
   * Returns the filter view with its parameters (see 'Sample' section)
   *
   * @param fqdn the requested table
   * @return
   */
  @RequestMapping(value = "/table/view/samplefilter", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getSamplefilter(String fqdn) {
    ModelAndView mav = new ModelAndView("body/table/sections/sampleFilter");
    MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
    Map<String, Set<String>> parameterValues = metascopeTableService.getParameterValues(table);
    mav.addObject("table", table);
    mav.addObject("parameterValues", parameterValues);
    return mav;
  }

  private String getParameter(HttpServletRequest request, String parameterKey) {
    Map<String, ?> inputFlashMap = RequestContextUtils.getInputFlashMap(request);
    String[] parameterValues = request.getParameterMap().get(parameterKey);
    if (parameterValues != null && parameterValues.length > 0) {
      return parameterValues[0];
    }
    if (inputFlashMap != null) {
      Object val = inputFlashMap.get(parameterKey);
      if (val != null) {
        return String.valueOf(val);
      }
    }
    return null;
  }

  public boolean hasSearchParameters(String url) {
    final Map<String, List<String>> query_pairs = new LinkedHashMap<String, List<String>>();
    final String[] pairs =url.split("&");
    for (String pair : pairs) {
      final int idx = pair.indexOf("=");
      try {
        final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
        if (!query_pairs.containsKey(key)) {
          query_pairs.put(key, new LinkedList<String>());
        }
        final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
        query_pairs.get(key).add(value);
      } catch (UnsupportedEncodingException e) {
        LOG.warn("Could not parse URL", e);
      }
    }
    for (Entry<String, List<String>> e : query_pairs.entrySet()) {
      if (!e.getKey().equals("e") && !e.getKey().equals("p")) {
        if (!e.getKey().equals("searchQuery") || (e.getKey().equals("searchQuery") && !e.getValue().isEmpty())) {
          return true;
        }
      }
    }
    return false;
  }

}