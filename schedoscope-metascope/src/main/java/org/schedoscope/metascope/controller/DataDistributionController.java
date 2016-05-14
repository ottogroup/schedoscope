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

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.service.DataDistributionService;
import org.schedoscope.metascope.service.TableEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class DataDistributionController {

	@Autowired
	private TableEntityService tableEntityService;
	@Autowired
	private DataDistributionService dataDistributionService;

	@RequestMapping("/process/view")
	@ResponseBody
	@Transactional
	public String startJobsForView(HttpServletRequest request, String urlpath) {
		boolean jobsStarted = dataDistributionService
				.calculateDistributionForView(urlpath);
		if (jobsStarted) {
			return "Scheduled jobs for view " + urlpath;
		} else {
			return "View " + urlpath + " not found.";
		}
	}

	@RequestMapping("/process/table")
	@ResponseBody
	@Transactional
	public String startJobsForTable(HttpServletRequest request, String fqdn) {
		TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
		List<ViewEntity> views = tableEntity.getViews();
		for (ViewEntity viewEntity : views) {
			String view = viewEntity.getUrlPath();
			dataDistributionService.calculateDistributionForView(view);
		}
		return "Scheduled jobs for " + views.size() + " views from table "
				+ fqdn;
	}

}
