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
package org.schedoscope.metascope.schedoscope.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ViewStatus {

	private Map<String, String> overview;
	private List<View> views;

	public Map<String, String> getOverview() {
		return overview;
	}

	public void setOverview(Map<String, String> overview) {
		this.overview = overview;
	}

	public List<View> getViews() {
		return views;
	}

	public void setViews(List<View> views) {
		this.views = views;
	}

	public List<String> getTableNames() {
		List<String> tableNames = new ArrayList<String>();
		for (View view : views) {
			if (!tableNames.contains(view.getFqdn())) {
				tableNames.add(view.getFqdn());
			}
		}
		return tableNames;
	}

}
