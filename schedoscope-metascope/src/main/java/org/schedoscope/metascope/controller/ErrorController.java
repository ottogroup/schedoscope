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
import org.schedoscope.metascope.service.UserEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class ErrorController extends ViewController {

  private static final String TEMPLATE_ERROR = "util";

  @Autowired
  private UserEntityService userEntityService;

  @Autowired
  private MetascopeConfig config;

  @RequestMapping("/accessdenied")
  public ModelAndView accessdenied(HttpServletRequest request) {
    return showErrorView("accessdenied");
  }

  @RequestMapping("/notfound")
  public ModelAndView notfound(HttpServletRequest request) {
    return showErrorView("notfound");
  }

  @RequestMapping("/expired")
  public ModelAndView expired(HttpServletRequest request) {
    return showErrorView("expired");
  }

  @Override
  protected String getTemplateUri() {
    return TEMPLATE_ERROR;
  }

  private ModelAndView showErrorView(String view) {
    ModelAndView mav = createView(view);
    mav.addObject("userEntityService", userEntityService);
    if (userEntityService.isAuthenticated()) {
      mav.addObject("admin", userEntityService.isAdmin());
      mav.addObject("userMgmnt", config.withUserManagement());
    }
    return mav;
  }
}
