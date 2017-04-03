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

import org.apache.commons.validator.routines.EmailValidator;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.model.MetascopeUser;
import org.schedoscope.metascope.service.MetascopeMetadataService;
import org.schedoscope.metascope.service.MetascopeUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;

@Controller
public class MetascopeUserController {

    private static final String ADMIN_TEMPLATE = "body/admin/user";

    @Autowired
    private MetascopeUserService metascopeUserService;
    @Autowired
    private SessionRegistry sessionRegistry;
    @Autowired
    private MetascopeMetadataService metascopeMetadataService;
    @Autowired
    private MetascopeConfig config;

    @RequestMapping(value = "/admin/users", method = RequestMethod.GET)
    public ModelAndView getUserManagement(HttpServletRequest request) {
        ModelAndView mav = new ModelAndView("body/admin/user/user_management");

        boolean admin = metascopeUserService.isAdmin();
        boolean withUserManagement = config.withUserManagement();
        String schedoscopeTimestamp = metascopeMetadataService.getMetadataValue("timestamp");
        Iterable<MetascopeUser> users = metascopeUserService.getAllUser();

        mav.addObject("schedoscopeTimestamp", schedoscopeTimestamp);
        mav.addObject("userEntityService", metascopeUserService);
        mav.addObject("admin", admin);
        mav.addObject("userMgmnt", withUserManagement);
        mav.addObject("users", users);

        return mav;
    }

    @RequestMapping(value = "/admin/user/create", method = RequestMethod.POST)
    public ModelAndView createUser(HttpServletRequest request, String username, String email, String fullname,
                                   String password, boolean admin, String group) {
        ModelAndView mav = new ModelAndView("body/admin/user/index");

        if (email != null && !EmailValidator.getInstance().isValid(email)) {
            return mav.addObject("invalidEmail", true).addObject("service", metascopeUserService);
        }

        if (metascopeUserService.userExists(username)) {
            return mav.addObject("userExists", true).addObject("service", metascopeUserService);
        } else if (metascopeUserService.emailExists(email)) {
            return mav.addObject("emailExists", true).addObject("service", metascopeUserService);
        }

        this.metascopeUserService.createUser(username, email, fullname, password, admin, group);
        return new ModelAndView(new RedirectView(request.getHeader("Referer")));
    }

    @RequestMapping(value = "/admin/user/edit", method = RequestMethod.POST)
    public ModelAndView editUser(HttpServletRequest request, String username, String email, String fullname,
                                 String password, boolean admin, String group) {
        this.metascopeUserService.editUser(username, email, fullname, password, admin, group);
        this.metascopeUserService.logoutUser(sessionRegistry, username);
        return new ModelAndView(new RedirectView(request.getHeader("Referer")));
    }

    @RequestMapping(value = "/admin/user/delete", method = RequestMethod.POST)
    public ModelAndView deleteUser(HttpServletRequest request, String username) {
        this.metascopeUserService.deleteUser(username);
        this.metascopeUserService.logoutUser(sessionRegistry, username);
        return new ModelAndView(new RedirectView(request.getHeader("Referer")));
    }

}
