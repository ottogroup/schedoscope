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

import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.repository.MetascopeTableRepository;
import org.schedoscope.metascope.service.MetascopeDataDistributionService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
public class MetascopeDataDistributionController {

  @Autowired
  private MetascopeTableService metascopeTableService;

  @Autowired
  private MetascopeDataDistributionService metascopeDataDistributionService;

  @RequestMapping("/datadistribution/start")
  public String calculateDistribution(HttpServletRequest request, String fqdn) {
    if (fqdn != null) {
      MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
      if (table != null) {
        MetascopeDataDistributionService.Status status = metascopeDataDistributionService.checkStatus(table);
        if (status != null && status.equals(MetascopeDataDistributionService.Status.NotAvailable)) {
          metascopeDataDistributionService.calculateDistribution(table);
        }
      }
    }
    return "redirect:" + request.getHeader("Referer") + "#datadistributionContent";
  }

}
