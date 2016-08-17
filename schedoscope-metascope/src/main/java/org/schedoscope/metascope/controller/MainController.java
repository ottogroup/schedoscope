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
package org.schedoscope.metascope.controller;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.index.model.SolrQueryResult;
import org.schedoscope.metascope.service.*;
import org.schedoscope.metascope.util.HTMLUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
public class MainController extends ViewController {

    private static final String TEMPLATE_HOME = "body/home";

    @Autowired
    private ActivityEntityService activityEntityService;
    @Autowired
    private UserEntityService userEntityService;
    @Autowired
    private TableEntityService tableEntityService;
    @Autowired
    private TaxonomyService taxonomyService;
    @Autowired
    private MetadataEntityService metadataEntityService;
    @Autowired
    private SolrFacade solrIndex;
    @Autowired
    private URLService urlService;
    @Autowired
    private MetascopeConfig config;
    @Autowired
    private HTMLUtil htmlUtil;

    @Override
    protected String getTemplateUri() {
        return TEMPLATE_HOME;
    }

    /**
     * Login page of Metascope
     *
     * @param request
     *          the HTTPServlet request
     * @param params
     *          params passed from the client
     * @return the login view
     */
    @RequestMapping("/")
    public ModelAndView index(HttpServletRequest request, @RequestParam Map<String, String> params) {
        ModelAndView mav = createView("index");

        boolean authenticationFailure = params.containsKey("error");
        setupView(mav, authenticationFailure);

        if (userEntityService.isAuthenticated()) {
            return new ModelAndView(new RedirectView("/home"));
        }

        return mav;
    }

    /**
     * Landing page of Metascope
     *
     * @param request
     *          the HTTPServlet request
     * @param params
     *          params passed from the client
     * @return the main view
     */
    @RequestMapping("/home")
    public ModelAndView home(HttpServletRequest request, @RequestParam Map<String, String> params) {
        ModelAndView mav = createView("index");
        setupView(mav, false);

        if (userEntityService.isAuthenticated()) {
      /* get solr information for search and facets and query results */
            SolrQueryResult solrQueryResult = solrIndex.query(params);

      /*
       * solr query results (facets, filter and the actual entities to display
       */
            mav.addObject("solrQuery", solrQueryResult);

      /* reference to URLService to build the filter (facets) url's */
            mav.addObject("urlService", urlService);

      /* most viewed tables */
            mav.addObject("topFive", tableEntityService.getTopFiveTables());

      /* last activities to display on landing page */
            mav.addObject("activities", activityEntityService.getActivities());

      /* check wether the user is an admin or not */
            mav.addObject("admin", userEntityService.isAdmin());

      /* favourited tables of this user */
            mav.addObject("favs", userEntityService.getFavourites());

      /* utility for styling */
            mav.addObject("util", htmlUtil);

      /* user management enabled/disabled */
            mav.addObject("userMgmnt", config.withUserManagement());

      /* objects needed for admin views */
            if (userEntityService.getUser().isAdmin()) {
        /* user management */
                mav.addObject("users", userEntityService.getAllUser());

        /* taxonomy management */
                mav.addObject("taxonomies", taxonomyService.getTaxonomies());
            }

        }

        return mav;
    }

    /**
     * Sets the mandatory objects for the main view
     *
     * @param mav
     *          the ModelAndView object to add objects to
     * @param authenticationFailure
     *          if user tried to login unsuccessfully
     */
    private void setupView(ModelAndView mav, boolean authenticationFailure) {
    /* last schedoscope sync */
        mav.addObject("schedoscopeTimestamp", metadataEntityService.getMetadataValue("timestamp"));

    /* reference to user entity service to check users groups on the fly */
        mav.addObject("userEntityService", userEntityService);

        if (authenticationFailure) {
            mav.addObject("error", true);
        }
    }

}