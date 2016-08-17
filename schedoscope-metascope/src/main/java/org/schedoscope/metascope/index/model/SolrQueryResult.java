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
package org.schedoscope.metascope.index.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SolrQueryResult {

    /** */
    private List<SolrQueryResultEntity> resultEntities;

    /**
     * map of all filter (key = name to display; value = actual parameter to
     * filter on)
     */
    private List<SolrQueryParameter> filter;

    /** the facet values (name and count) for the filters */
    private Map<String, List<SolrFacet>> facetValues;

    /** user selections; (key = filter name, value = list of selections) */
    private Map<String, List<String>> activeFilterValues;

    /** the users searach query */
    private String searchQuery;

    /** the current page */
    private int page;

    /** the current number of elements which are shown on one page */
    private int elements;

    /** the total number of pages */
    private int totalPages;

    /** the total number of entities found */
    private long totalResults;

    /** either Table or Partition */
    private String resultType;

    public SolrQueryResult() {
    }

    public SolrQueryResult withResultEntities(List<SolrQueryResultEntity> resultEntities) {
        this.resultEntities = resultEntities;
        Collections.sort(resultEntities);
        return this;
    }

    public List<SolrQueryResultEntity> getResultEntities() {
        return resultEntities;
    }

    public List<SolrQueryParameter> getFilters() {
        return filter;
    }

    public SolrQueryResult withFilters(List<SolrQueryParameter> filter) {
        this.filter = filter;
        return this;
    }

    public Map<String, List<SolrFacet>> getFacetValues() {
        return facetValues;
    }

    public SolrQueryResult withFacetValues(Map<String, List<SolrFacet>> facetValues) {
        this.facetValues = facetValues;
        return this;
    }

    public Map<String, List<String>> getActiveFilterValues() {
        return activeFilterValues;
    }

    public SolrQueryResult withActiveFilterValues(Map<String, List<String>> activeFilterValues) {
        this.activeFilterValues = activeFilterValues;
        return this;
    }

    public String getSearchQuery() {
        return searchQuery;
    }

    public SolrQueryResult withSearchQuery(String searchQuery) {
        this.searchQuery = searchQuery;
        return this;
    }

    public int getPage() {
        return page;
    }

    public SolrQueryResult withPage(int page) {
        this.page = page;
        return this;
    }

    public int getElements() {
        return elements;
    }

    public SolrQueryResult withElements(int elements) {
        this.elements = elements;
        return this;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public SolrQueryResult withTotalPages(int totalPages) {
        this.totalPages = totalPages == 0 ? 1 : totalPages;
        return this;
    }

    public long getTotalResults() {
        return totalResults;
    }

    public SolrQueryResult withTotalResults(long l) {
        this.totalResults = l;
        return this;
    }

    public SolrQueryResult withResultType(String resultType) {
        this.resultType = resultType;
        return this;
    }

    public String getResultType() {
        return resultType;
    }

    public long getStart() {
        return ((page - 1) * elements) + 1;
    }

    public long getEnd() {
        int end = ((page - 1) * elements) + elements;
        return end < totalResults ? end : totalResults;
    }

    public boolean hasActiveFilters() {
        for (List<String> v : activeFilterValues.values()) {
            if (v.size() > 0) {
                return true;
            }
        }
        return false;
    }

    public boolean hasFacetValues(String facet) {
        List<SolrFacet> facets = facetValues.get(facet);
        if (facets == null) {
            return false;
        }

        for (SolrFacet solrFacet : facets) {
            if (solrFacet.getCount() > 0) {
                return true;
            }
        }
        return false;
    }

    public long getCountForFacetAndKey(String facet, String key) {
        List<SolrFacet> list = facetValues.get(facet);
        if (list == null) {
            return -1L;
        }
        for (SolrFacet solrFacet : list) {
            if (solrFacet.getName().equalsIgnoreCase(key)) {
                return solrFacet.getCount();
            }
        }
        return -1L;
    }

    public boolean hasSearchQuery() {
        return !searchQuery.equals("*");
    }
}
