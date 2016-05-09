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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.CategoryMap;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.TaxonomyEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.service.DataDistributionService;
import org.schedoscope.metascope.service.TableEntityService;
import org.schedoscope.metascope.service.TaxonomyService;
import org.schedoscope.metascope.service.UserEntityService;
import org.schedoscope.metascope.service.ViewEntityService;
import org.schedoscope.metascope.util.HTMLUtil;
import org.schedoscope.metascope.util.HiveQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
public class TableEntityController extends ViewController {

  private static final Logger LOG = LoggerFactory.getLogger(TableEntityController.class);

  private static final String TEMPLATE_TABLES = "body/table";

  @Autowired
  private TableEntityService tableEntityService;
  @Autowired
  private ViewEntityService viewEntityService;
  @Autowired
  private UserEntityService userEntityService;
  @Autowired
  private TaxonomyService taxonomyService;
  @Autowired
  private DataDistributionService dataDistributionService;
  @Autowired
  private MetascopeConfig config;
  @Autowired
  private HTMLUtil htmlUtil;

  @Override
  protected String getTemplateUri() {
    return TEMPLATE_TABLES;
  }

  /**
   * Detail view of a table; path = '/table'
   * 
   * @param request
   *          the HTTPServletRequest from the client
   * @param fqdn
   *          the fqdn (fully qualified domain name) of the table to display
   * @param selectedPartition
   *          the partition which has been selected in 'Data Distribution'
   *          section
   * @param partitionPage
   *          the partition page the user requested in the 'Partitions' section
   * @param transitive
   *          show transitive dependencies in the 'Dependency'section
   * @param local
   *          Indicator if the request comes from Metascope or a third party
   *          application. If false, the css information will be included in the
   *          view
   * @return the table detail view
   */
  @RequestMapping(value = "/table", method = RequestMethod.GET)
  public ModelAndView getTable(HttpServletRequest request, String fqdn, String selectedPartition,
      Integer partitionPage, boolean transitive, boolean local) {
    ModelAndView mav = createView("table");

    /* load table from repository */
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);

    if (tableEntity == null) {
      return notFound();
    }

    /* partition page defaults to 1 */
    if (partitionPage == null || partitionPage < 1) {
      partitionPage = 1;
    }

    /* get all taxonomies */
    Iterable<TaxonomyEntity> taxonomies = taxonomyService.getTaxonomies();
    List<String> taxonomyNames = new ArrayList<String>();
    for (TaxonomyEntity taxonomyEntity : taxonomies) {
    	taxonomyNames.add(taxonomyEntity.getName());
    }
    
    Map<String, CategoryMap> tableTaxonomies = tableEntityService.getTableTaxonomies(tableEntity);

    /* get all users for user management and owner auto completion */
    Iterable<UserEntity> users = userEntityService.getAllUser();

    /* get all registered table owners for auto completion */
    Set<String> owner = tableEntityService.getAllOwner();
    owner.remove(null);
    for (UserEntity user : users) {
      if (!owner.contains(user.getFullname())) {
        owner.add(user.getFullname());
      }
    }

    /* get partition count of the table */
    int partitionCount = viewEntityService.getPartitionCount(tableEntity);

    /* get viewentity for data distribution, if selected */
    ViewEntity selectedViewEntity = tableEntityService.runDataDistribution(tableEntity, selectedPartition,
        partitionCount);

    /* check if this user is an admin */
    boolean isAdmin = userEntityService.isAdmin();

    /* check if this table is favourised */
    boolean isFavourite = userEntityService.isFavourite(tableEntity);

    if (transitive) {
      /* get transitive dependencies and successors */
      List<TableDependencyEntity> transitiveDependencies = tableEntityService.getTransitiveDependencies(tableEntity);
      List<TableDependencyEntity> transitiveSuccessors = tableEntityService.getTransitiveSuccessors(tableEntity);
      mav.addObject("transitiveDependencies", transitiveDependencies);
      mav.addObject("transitiveSuccessors", transitiveSuccessors);
    } else {
      /* get the successors of this table */
      List<TableDependencyEntity> successors = tableEntityService.getSuccessors(tableEntity);
      mav.addObject("successors", successors);
    }

    /*
     * if table is partitioned, get the first parameter (see 'Data Distribution'
     * section)
     */
    List<FieldEntity> parameters = tableEntity.getParameters();
    if (parameters.size() > 0) {
      mav.addObject("firstParam", parameters.get(0).getName());
    }

    /* make objects accessible for thymeleaf to render final view */
    mav.addObject("table", tableEntity);
    mav.addObject("userEntityService", userEntityService);
    mav.addObject("util", htmlUtil);
    mav.addObject("local", local);
    mav.addObject("partitionPage", partitionPage);
    mav.addObject("partitionCount", partitionCount);
    mav.addObject("selectedPartition", selectedViewEntity);
    mav.addObject("users", users);
    mav.addObject("owner", owner);
    mav.addObject("taxonomies", taxonomies);
    mav.addObject("taxonomyNames", taxonomyNames);
    mav.addObject("tableTaxonomies", tableTaxonomies);
    mav.addObject("admin", isAdmin);
    mav.addObject("isFavourite", isFavourite);
    mav.addObject("userMgmnt", config.withUserManagement());

    return mav;
  }

  /**
   * Adds or removes a table from users favourites
   * 
   * @param request
   *          the HTTPServletRequest from the client
   * @param fqdn
   *          the table to add/remove from favourites
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/table/favourite", method = RequestMethod.POST)
  public String addOrRemoveFavourite(HttpServletRequest request, String fqdn) {
    tableEntityService.addOrRemoveFavourite(fqdn);
    return "redirect:" + request.getHeader("Referer");
  }

  /**
   * Adds or removes the business objects from the table
   * 
   * @param request
   *          the HTTPServletRequest from the client
   * @param fqdn
   *          the table to add/remove the business objects
   * @param businessObjects
   *          The new business objects of the table
   * @param tags
   *          The new tags of the table
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/table/categoryobjects", method = RequestMethod.POST)
  public String addCategoryObject(HttpServletRequest request) {
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
  		tableEntityService.setCategoryObjects(fqdn, parameterMap);
  		tableEntityService.setTags(fqdn, tags);
  	}
    return "redirect:" + request.getHeader("Referer") + "#taxonomyContent";
  }

  /**
   * Changes the 'person responsible' of the table
   * 
   * @param request
   *          the HTTPServletRequest from the client
   * @param fqdn
   *          the table to be changed
   * @param person
   *          the fullname of the new person responsible
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/table/owner", method = RequestMethod.POST)
  public String changeOwner(HttpServletRequest request, String fqdn, String person) {
    tableEntityService.setPersonResponsible(fqdn, person);
    return "redirect:" + request.getHeader("Referer");
  }

  /**
   * Changes the 'admin' properties of the table
   * 
   * @param request
   *          request the HTTPServletRequest from the client
   * @param fqdn
   *          the table to be changed
   * @param dataTimestampField
   *          the new timestamp field of the table
   * @param dataTimestampFieldFormat
   *          the corresponding format for the timestamp
   * @return the same view the user request came from
   */
  @RequestMapping(value = "/admin/table", method = RequestMethod.POST)
  public String admin(HttpServletRequest request, String fqdn, String dataTimestampField,
      String dataTimestampFieldFormat) {
    tableEntityService.setTimestampField(fqdn, dataTimestampField, dataTimestampFieldFormat);
    return "redirect:" + request.getHeader("Referer") + "#adminContent";
  }

  /**
   * Retrieves datadistribution for a specific view
   * 
   * @param request
   *          the HTTPServletRequest from the client containing view parameter
   *          values
   * @param redirectAttributes
   * @return returns to table detail page (Data Distribution section)
   */
  @RequestMapping(value = "/table/datadistribution", method = RequestMethod.POST)
  public String requestDatadist(HttpServletRequest request, RedirectAttributes redirectAttributes) {
    Map<String, String[]> params = request.getParameterMap();
    String fqdn = params.get("fqdn")[0];
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
    if (tableEntity == null) {
      return "redirect:/notfound";
    }

    String partition = null;
    for (FieldEntity parameter : tableEntity.getParameters()) {
      String val = params.get(parameter.getName())[0];
      if (partition == null) {
        partition = tableEntity.getUrlPathPrefix() + val;
      } else {
        partition += "/" + val;
      }
    }

    redirectAttributes.addFlashAttribute("request", request);
    redirectAttributes.addAttribute("fqdn", fqdn);
    redirectAttributes.addAttribute("selectedPartition", partition);
    return "redirect:/table";
  }

  /**
   * Increases the view count by one
   * 
   * @param fqdn
   *          the table
   */
  @RequestMapping(value = "/table/viewcount", method = RequestMethod.POST)
  @ResponseStatus(value = HttpStatus.OK)
  public void increaseViewCount(String fqdn) {
    tableEntityService.increaseViewCount(fqdn);
  }

  /**
   * Returns a JSON for VisJS to render the lineage graph
   * 
   * @param fqdn
   *          the table for which the lineage graph is requesed
   * @return JSON for VisJS
   */
  @RequestMapping(value = "/table/view/lineage", method = RequestMethod.GET)
  @ResponseBody
  public String getLineage(String fqdn) {
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);

    if (tableEntity == null) {
      return failed();
    }

    return tableEntityService.getLineage(tableEntity);
  }

  /**
   * Returns the lineage detail when a node is selected in the lineage graph
   * 
   * @param fqdn
   *          the table for which the lineage graph is requesed
   * @param type
   *          the type of the node (either transformation or table)
   * @return
   */
  @RequestMapping(value = "/table/view/lineage/detail", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getTableDetail(String fqdn, String type) {
    ModelAndView mav = null;
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
    if (tableEntity != null) {
      mav = createView("sections/lineagedetail");
      mav.addObject("table", tableEntity);
      mav.addObject("type", type);
      mav.addObject("util", htmlUtil);
      mav.addObject("userEntityService", userEntityService);
    }
    return mav;
  }

  /**
   * Return a HTML table containing a data sample
   * 
   * @param fqdn
   *          the table for which the sample is requested
   * @param params
   *          partition parameter for filtering
   * @return
   */
  @RequestMapping(value = "/table/view/sample", method = RequestMethod.GET)
  @ResponseBody
  public String getSample(String fqdn, @RequestParam Map<String, String> params) {
    if (fqdn == null) {
      return null;
    }

    params.remove("fqdn");
    
    Future<HiveQueryResult> future = tableEntityService.getSample(fqdn, params);
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
   * @param fqdn
   *          the parent table of the partitions
   * @param partitionPage
   *          the requested partition page
   * @return a HTML table with sample data
   */
  @RequestMapping(value = "/table/view/views", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getViews(String fqdn, int partitionPage) {
    ModelAndView mav = createView("sections/views");
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
    Page<ViewEntity> views = tableEntityService.getRequestedViewPage(fqdn, new PageRequest(partitionPage - 1, 20));
    mav.addObject("table", tableEntity);
    mav.addObject("views", views);
    mav.addObject("util", htmlUtil);
    return mav;
  }

  /**
   * Returns the filter view with its parameters (see 'Sample' section)
   * 
   * @param fqdn
   *          the requested table
   * @return
   */
  @RequestMapping(value = "/table/view/samplefilter", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getSamplefilter(String fqdn) {
    ModelAndView mav = createView("sections/sampleFilter");
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
    Map<String, List<String>> parameterValues = tableEntityService.getParameterValues(tableEntity);
    mav.addObject("table", tableEntity);
    mav.addObject("parameterValues", parameterValues);
    return mav;
  }

  /**
   * Returns a filter row with possible values for the 'Data Distribution'
   * filter
   * 
   * @param request
   *          the request containing all parameters set by the user
   * @return a HTML snippet containing all possible values
   */
  @RequestMapping(value = "/table/view/parametervalues", method = RequestMethod.GET)
  @ResponseBody
  public ModelAndView getParameterValues(HttpServletRequest request) {
    ModelAndView mav = createView("sections/datadisFilter");
    Map<String, String[]> parameterMap = request.getParameterMap();
    String fqdn = null;
    if (parameterMap.get("fqdn") != null) {
      fqdn = parameterMap.get("fqdn")[0];
    }
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
    if (tableEntity == null) {
      return mav;
    }

    List<FieldEntity> tableParameters = tableEntity.getParameters();

    if (tableParameters.size() == 0) {
      return mav;
    }

    String parameter = null;
    int parameterOrder = -1;
    String next = null;

    for (Entry<String, String[]> e : parameterMap.entrySet()) {
      if (e.getKey().equals("fqdn")) {
        continue;
      } else if (e.getKey().equals("next")) {
        parameter = e.getValue()[0];
      }
    }

    String urlPrefix = tableEntity.getUrlPathPrefix();
    boolean foundCurrentParamter = false;
    for (FieldEntity fieldEntity : tableParameters) {
      for (Entry<String, String[]> e : parameterMap.entrySet()) {
        if (fieldEntity.getName().equals(e.getKey())) {
          String value = parameterMap.get(e.getKey())[0];
          if (!value.isEmpty()) {
            urlPrefix += parameterMap.get(e.getKey())[0] + "/";
          }
        }
      }
      if (foundCurrentParamter) {
        next = fieldEntity.getName();
        break;
      }
      if (parameter != null && !parameter.isEmpty()) {
        if (fieldEntity.getName().equals(parameter)) {
          foundCurrentParamter = true;
          parameterOrder = fieldEntity.getFieldOrder();
        }
      }
    }

    if (parameter == null || parameter.isEmpty()) {
      parameter = tableParameters.get(0).getName();
    }

    Set<String> parameterValues = tableEntityService.getParameterValues(tableEntity, urlPrefix, parameter);
    mav.addObject("table", tableEntity);
    mav.addObject("parameter", parameter);
    mav.addObject("parameterOrder", parameterOrder);
    mav.addObject("next", next);
    mav.addObject("parameterValues", parameterValues);
    return mav;
  }

}