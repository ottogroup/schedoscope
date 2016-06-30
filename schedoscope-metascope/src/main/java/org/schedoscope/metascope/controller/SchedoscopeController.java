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

import org.schedoscope.metascope.service.SchedoscopeCommandService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class SchedoscopeController {

  @Autowired
  private SchedoscopeCommandService schedoscopeCommandService;

  @RequestMapping("/admin/schedoscope")
  public String invalidate(HttpServletRequest request, String action, String urlPath) {
    if (action.equals(SchedoscopeCommandService.INVALIDATE_COMMAND)) {
      this.schedoscopeCommandService.invalidateView(urlPath);
    } else if (action.equals(SchedoscopeCommandService.MATERIALIZE_COMMAND)) {
      this.schedoscopeCommandService.materializeView(urlPath);
    }
    return "redirect:" + request.getHeader("Referer");
  }

}
