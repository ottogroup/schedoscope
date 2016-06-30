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

public class SolrQueryParameter {

  public enum FilterType {
    AND, OR, EXCLUSIVE
  }

  public enum FacetSort {
    COUNT, INDEX
  }

  private String displayName;
  private String name;
  private boolean excludeFromFacet;
  private FilterType filterType;
  private FacetSort facetSort;

  public SolrQueryParameter(String displayName, String name, FilterType filterType) {
    this(displayName, name, false, filterType, FacetSort.INDEX);
  }

  public SolrQueryParameter(String displayName, String name, FilterType filterType, FacetSort facetSort) {
    this(displayName, name, false, filterType, facetSort);
  }

  public SolrQueryParameter(String displayName, String name, boolean excludeFromFacet, FilterType filterType,
      FacetSort facetSort) {
    this.displayName = displayName;
    this.name = name;
    this.excludeFromFacet = excludeFromFacet;
    this.filterType = filterType;
    this.facetSort = facetSort;
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

  public boolean isExcludeFromFacet() {
    return excludeFromFacet;
  }

  public void setExcludeFromFacet(boolean excludeFromFacet) {
    this.excludeFromFacet = excludeFromFacet;
  }

  public FilterType getFilterType() {
    return filterType;
  }

  public void setFilterType(FilterType filterType) {
    this.filterType = filterType;
  }

  public FacetSort getFacetSort() {
    return facetSort;
  }

  public void setFacetSort(FacetSort facetSort) {
    this.facetSort = facetSort;
  }

  public boolean isExclusiveFilter() {
    return filterType.equals(FilterType.EXCLUSIVE);
  }

}
