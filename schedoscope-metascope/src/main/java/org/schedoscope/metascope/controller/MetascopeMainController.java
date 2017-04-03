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
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.index.model.SolrQueryResult;
import org.schedoscope.metascope.service.*;
import org.schedoscope.metascope.util.HTMLUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.support.RequestContextUtils;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
public class MetascopeMainController {

    @Autowired
    private SolrFacade solrIndex;
    @Autowired
    private MetascopeUserService metascopeUserService;
    @Autowired
    private MetascopeTableService metascopeTableService;
    @Autowired
    private MetascopeTaxonomyService metascopeTaxonomyService;
    @Autowired
    private MetascopeMetadataService metascopeMetadataService;
    @Autowired
    private MetascopeActivityService metascopeActivityService;
    @Autowired
    private MetascopeURLService metascopeURLService;
    @Autowired
    private MetascopeConfig config;
    @Autowired
    private HTMLUtil htmlUtil;

    /**
     * Login page of Metascope
     *
     * @param request the HTTPServlet request
     * @param params  params passed from the client
     * @return the login view
     */
    @RequestMapping("/")
    public ModelAndView index(HttpServletRequest request, @RequestParam Map<String, String> params) {
        ModelAndView mav = new ModelAndView("body/home/index");

        boolean authenticationFailure = params.containsKey("error");
        setupView(mav, authenticationFailure);

        if (metascopeUserService.isAuthenticated()) {
            return new ModelAndView(new RedirectView("/home"));
        }

        return mav;
    }

    /**
     * Landing page of Metascope
     *
     * @param request the HTTPServlet request
     * @param params  params passed from the client
     * @return the main view
     */
    @RequestMapping("/home")
    @Transactional
    public ModelAndView home(HttpServletRequest request, @RequestParam Map<String, String> params) {
        ModelAndView mav = new ModelAndView("body/home/index");
        setupView(mav, false);

        if (metascopeUserService.isAuthenticated()) {
      /* get solr information for search and facets and query results */
            SolrQueryResult solrQueryResult = solrIndex.query(params);

      /*
       * solr query results (facets, filter and the actual entities to display
       */

            mav.addObject("solrQuery", solrQueryResult);

      /* reference to URLService to build the filter (facets) url's */
            mav.addObject("urlService", metascopeURLService);

      /* most viewed tables */
            mav.addObject("topFive", metascopeTableService.getTopFiveTables());

      /* last activities to display on landing page */
            mav.addObject("activities", metascopeActivityService.getActivities());

      /* check wether the user is an admin or not */
            mav.addObject("admin", metascopeUserService.isAdmin());

      /* favourited tables of this user */
            mav.addObject("favs", metascopeUserService.getFavourites());

      /* utility for styling */
            mav.addObject("util", htmlUtil);

      /* user management enabled/disabled */
            mav.addObject("userMgmnt", config.withUserManagement());

      /* check if the sync task has been scheduled */
            mav.addObject("schedoscopesync", getParameter(request, "schedoscopesync"));

      /* objects needed for admin views */
            if (metascopeUserService.getUser().isAdmin()) {
        /* user management */
                mav.addObject("users", metascopeUserService.getAllUser());

        /* taxonomy management */
                mav.addObject("taxonomies", metascopeTaxonomyService.getTaxonomies());
            }

        }

        return mav;
    }

    /**
     * Sets the mandatory objects for the main view
     *
     * @param mav                   the ModelAndView object to add objects to
     * @param authenticationFailure if user tried to login unsuccessfully
     */
    private void setupView(ModelAndView mav, boolean authenticationFailure) {
    /* last schedoscope sync */
        mav.addObject("schedoscopeTimestamp", metascopeMetadataService.getMetadataValue("schedoscopeTimestamp"));

    /* reference to user entity service to check users groups on the fly */
        mav.addObject("userEntityService", metascopeUserService);

        if (authenticationFailure) {
            mav.addObject("error", true);
        }
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

}