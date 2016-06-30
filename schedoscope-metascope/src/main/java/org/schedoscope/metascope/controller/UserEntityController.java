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

import org.apache.commons.validator.routines.EmailValidator;
import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.service.MetadataEntityService;
import org.schedoscope.metascope.service.UserEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

@Controller
public class UserEntityController extends ViewController {

  private static final String ADMIN_TEMPLATE = "body/admin/user";

  @Autowired
  private UserEntityService userEntityService;
  @Autowired
  private SessionRegistry sessionRegistry;
  @Autowired
  private MetadataEntityService metadataEntityService;
  @Autowired
  private MetascopeConfig config;

  @RequestMapping(value = "/admin/users", method = RequestMethod.GET)
  public ModelAndView getUserManagement(HttpServletRequest request) {
    ModelAndView mav = createView("user_management");

    boolean admin = userEntityService.isAdmin();
    boolean withUserManagement = config.withUserManagement();
    String schedoscopeTimestamp = metadataEntityService.getMetadataValue("timestamp");
    Iterable<UserEntity> users = userEntityService.getAllUser();

    mav.addObject("schedoscopeTimestamp", schedoscopeTimestamp);
    mav.addObject("userEntityService", userEntityService);
    mav.addObject("admin", admin);
    mav.addObject("userMgmnt", withUserManagement);
    mav.addObject("users", users);

    return mav;
  }

  @RequestMapping(value = "/admin/user/create", method = RequestMethod.POST)
  public ModelAndView createUser(HttpServletRequest request, String username, String email, String fullname,
      String password, boolean admin, String group) {
    ModelAndView mav = createView("index");

    if (email != null && !EmailValidator.getInstance().isValid(email)) {
      return mav.addObject("invalidEmail", true).addObject("service", userEntityService);
    }

    if (userEntityService.userExists(username)) {
      return mav.addObject("userExists", true).addObject("service", userEntityService);
    } else if (userEntityService.emailExists(email)) {
      return mav.addObject("emailExists", true).addObject("service", userEntityService);
    }

    this.userEntityService.createUser(username, email, fullname, password, admin, group);
    return new ModelAndView(new RedirectView(request.getHeader("Referer")));
  }

  @RequestMapping(value = "/admin/user/edit", method = RequestMethod.POST)
  public ModelAndView editUser(HttpServletRequest request, String username, String email, String fullname,
      String password, boolean admin, String group) {
    this.userEntityService.editUser(username, email, fullname, password, admin, group);
    this.userEntityService.logoutUser(sessionRegistry, username);
    return new ModelAndView(new RedirectView(request.getHeader("Referer")));
  }

  @RequestMapping(value = "/admin/user/delete", method = RequestMethod.POST)
  public ModelAndView deleteUser(HttpServletRequest request, String username) {
    this.userEntityService.deleteUser(username);
    this.userEntityService.logoutUser(sessionRegistry, username);
    return new ModelAndView(new RedirectView(request.getHeader("Referer")));
  }

  @Override
  protected String getTemplateUri() {
    return ADMIN_TEMPLATE;
  }

}
