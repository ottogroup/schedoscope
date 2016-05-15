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
package org.schedoscope.metascope.index.model;

import java.util.ArrayList;
import java.util.List;

public class SolrFacetQuery {

	private String displayName;
	private String name;
	private List<SolrHourRange> ranges;

	public SolrFacetQuery(String displayName, String name) {
		this.displayName = displayName;
		this.name = name;
		this.ranges = new ArrayList<SolrHourRange>();
	}

	public SolrFacetQuery(String displayName, String name,
			List<SolrHourRange> ranges) {
		this.displayName = displayName;
		this.name = name;
		this.ranges = ranges;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<SolrHourRange> getRanges() {
		return ranges;
	}

	public void setRanges(List<SolrHourRange> ranges) {
		this.ranges = ranges;
	}

	public SolrFacetQuery withRange(SolrHourRange range) {
		this.ranges.add(range);
		return this;
	}

}
