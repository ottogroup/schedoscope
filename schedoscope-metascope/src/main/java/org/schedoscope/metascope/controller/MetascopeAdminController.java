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

import org.schedoscope.metascope.service.MetascopeAdminService;
import org.schedoscope.metascope.util.TaskMutex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.servlet.http.HttpServletRequest;

@Controller
public class MetascopeAdminController {

  @Autowired
  private MetascopeAdminService metascopeAdminService;

  @Autowired
  private TaskMutex taskMutex;

  @RequestMapping("/admin/sync")
  public String sync(HttpServletRequest request, RedirectAttributes redirAttr) {
    String sync = "failed";
    if (!taskMutex.isSchedoscopeTaskRunning()) {
      metascopeAdminService.schedule();
      sync = "success";
    }
    redirAttr.addFlashAttribute("schedoscopesync", sync);
    return "redirect:" + request.getHeader("Referer");
  }

}
