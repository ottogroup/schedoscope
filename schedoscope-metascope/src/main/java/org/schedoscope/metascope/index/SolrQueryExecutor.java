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
package org.schedoscope.metascope.index;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SuggesterResponse;
import org.apache.solr.client.solrj.response.Suggestion;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.schedoscope.metascope.index.model.*;
import org.schedoscope.metascope.index.model.SolrQueryParameter.FacetSort;
import org.schedoscope.metascope.index.model.SolrQueryParameter.FilterType;
import org.schedoscope.metascope.service.MetascopeFieldService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.schedoscope.metascope.service.MetascopeViewService;
import org.schedoscope.metascope.util.URLUtil;

import java.util.*;
import java.util.Map.Entry;

//TODO this class should be refactored..
public class SolrQueryExecutor {

  public static final String ID = "id";
  public static final String TYPE = "type";
  public static final String SCHEDOSCOPE_ID = "schedoscopeId";
  public static final String DATABASE_NAME = "databaseName";
  public static final String TABLE_NAME = "tableName";
  public static final String TAXONOMIES = "taxonomies";
  public static final String CATEGORIES = "categories";
  public static final String CATEGORY_OBJECTS = "categoryObjects";
  public static final String STORAGE_FORMAT = "storageFormat";
  public static final String TRANSFORMATION = "transformation";
  public static final String EXPORTS = "exports";
  public static final String TRANSFORMATIONTIMESTAMP = "transformationTimestamp";
  public static final String CREATED_AT = "createdAt";
  public static final String TAGS = "tags";
  public static final String STATUS = "status";
  public static final String TYPE_TABLE = "Table";
  public static final String TYPE_PARTITION = "Partition";
  public static final String FILTER_TYPE = "Type";
  public static final String FILTER_SCHEDOSCOPE = "Schedoscope Instance";
  public static final String FILTER_DATABASE = "Database";
  public static final String FILTER_TABLE = "Table";
  public static final String FILTER_STATUS = "Status";
  public static final String FILTER_TRANSFORMATION = "Transformation";
  public static final String FILTER_EXPORT = "Export";
  public static final String FILTER_STORAGEFORMAT = "Storage Format";
  public static final String FILTER_TAXONOMY = "Taxonomy";
  public static final String FILTER_CATEGORIES = "Category";
  public static final String FILTER_CATEGORY_OBJECT = "Category Object";
  public static final String FILTER_TAGS = "Tags";

  private final MetascopeTableService metascopeTableService;
  private final MetascopeViewService metascopeViewService;
  private final MetascopeFieldService metascopeParameterService;

  private SolrClient solrClient;
  private List<SolrQueryParameter> facetFields;
  private List<SolrFacetQuery> facetQueries;

  public SolrQueryExecutor(SolrClient solrClient, MetascopeTableService metascopeTableService,
                           MetascopeViewService metascopeViewService, MetascopeFieldService metascopeParameterService) {
    this.solrClient = solrClient;
    this.metascopeTableService = metascopeTableService;
    this.metascopeViewService = metascopeViewService;
    this.metascopeParameterService = metascopeParameterService;
    this.facetFields = new LinkedList<SolrQueryParameter>();
    this.facetQueries = new LinkedList<SolrFacetQuery>();
    this.facetFields.add(new SolrQueryParameter(FILTER_SCHEDOSCOPE, SCHEDOSCOPE_ID, false, FilterType.AND, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_DATABASE, DATABASE_NAME, true, FilterType.OR, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_TABLE, TABLE_NAME, false, FilterType.AND, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_STATUS, STATUS, true, FilterType.EXCLUSIVE, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_TRANSFORMATION, TRANSFORMATION, true, FilterType.EXCLUSIVE,
            FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_EXPORT, EXPORTS, true, FilterType.EXCLUSIVE, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_STORAGEFORMAT, STORAGE_FORMAT, true, FilterType.EXCLUSIVE,
            FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_TAXONOMY, TAXONOMIES, false, FilterType.AND, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_CATEGORIES, CATEGORIES, false, FilterType.AND, FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_CATEGORY_OBJECT, CATEGORY_OBJECTS, false, FilterType.AND,
            FacetSort.COUNT));
    this.facetFields.add(new SolrQueryParameter(FILTER_TAGS, TAGS, false, FilterType.AND, FacetSort.COUNT));
    this.facetQueries.add(new SolrFacetQuery("Creation Time", CREATED_AT)
            .withRange(new SolrHourRange("last 60 minutes", 1)).withRange(new SolrHourRange("last 24 hours", 24))
            .withRange(new SolrHourRange("last 7 days", 168)).withRange(new SolrHourRange("last month", 672))
            .withRange(new SolrHourRange("last year", 8760)).withRange(new SolrHourRange("older", Long.MAX_VALUE)));
    this.facetQueries.add(new SolrFacetQuery("Transformation Time", TRANSFORMATIONTIMESTAMP)
            .withRange(new SolrHourRange("last 60 minutes", 1)).withRange(new SolrHourRange("last 24 hours", 24))
            .withRange(new SolrHourRange("last 7 days", 168)).withRange(new SolrHourRange("last month", 672))
            .withRange(new SolrHourRange("last year", 8760)).withRange(new SolrHourRange("older", Long.MAX_VALUE)));
  }

  public List<String> suggest(String userInput) {
    List<String> suggestions = new LinkedList<String>();

    SolrQuery query = new SolrQuery();
    query.setParam(CommonParams.QT, "/suggest");
    query.setParam("suggest", true);
    query.setParam(SuggesterParams.SUGGEST_BUILD, true);
    query.setParam(SuggesterParams.SUGGEST_DICT, "metascope");
    query.setParam(SuggesterParams.SUGGEST_Q, userInput);

    /* execute the query */
    QueryResponse queryResponse = null;
    try {
      queryResponse = solrClient.query(query);
    } catch (Exception e) {
      e.printStackTrace();
    }

    List<Suggestion> currentSuggestions = new LinkedList<Suggestion>();
    if (queryResponse != null) {
      SuggesterResponse suggestorRespone = queryResponse.getSuggesterResponse();
      if (suggestorRespone != null) {
        Map<String, List<Suggestion>> suggestorResponeMap = suggestorRespone.getSuggestions();
        for (Entry<String, List<Suggestion>> e : suggestorResponeMap.entrySet()) {
          for (Suggestion suggestion : e.getValue()) {
            int counter = 0;
            for (Suggestion s : currentSuggestions) {
              if (s.getWeight() > suggestion.getWeight()) {
                counter++;
              }
            }
            currentSuggestions.add(counter, suggestion);
          }
        }
      }
    }

    for (Suggestion suggestion : currentSuggestions) {
      suggestions.add(suggestion.getTerm());
    }

    return suggestions;
  }

  /**
   * Perform a query on the metascope solr index. Builds up the query string
   * from the given parameters and sends request to solr server
   *
   * @param params the parameters which are included in the filter query
   * @return a SolrQueryResult object containing the result of the query
   */
  public SolrQueryResult query(Map<String, String> params) {
    SolrQuery query = new SolrQuery();

    /* enable facetting */
    query.setFacet(true);

    /* list of all filters; key: name to display, value: entity parameter */
    List<SolrQueryParameter> filter = new ArrayList<SolrQueryParameter>();

    /* list of partition filter */
    List<SolrQueryParameter> partitionFilter = new ArrayList<SolrQueryParameter>();

    /*
     * list of filter which have been set by the user (key = filtername, value =
     * list of selections)
     */
    Map<String, List<String>> activeFilterValues = new HashMap<String, List<String>>();

    /*
     * determine on which type is searched for (either 'Table' or 'Partition')
     */
    String typeFilterValue = params.get(TYPE);
    filter.add(new SolrQueryParameter(FILTER_TYPE, TYPE, FilterType.EXCLUSIVE, FacetSort.COUNT));
    if (typeFilterValue != null && typeFilterValue.equalsIgnoreCase(TYPE_PARTITION)) {
      typeFilterValue = TYPE_PARTITION;
      for (String parameter : metascopeParameterService.findDistinctParameters()) {
        partitionFilter.add(new SolrQueryParameter("Parameter: " + parameter, parameter + "_s", FilterType.AND,
                FacetSort.INDEX));
      }
      for (SolrQueryParameter pFilter : partitionFilter) {
        query.addFacetField(pFilter.getName());
        query.add("f." + pFilter.getName() + ".facet.sort", "index");
        String filterValue = params.get(pFilter.getName());
        if (filterValue != null && !filterValue.isEmpty()) {
          query.addFilterQuery(pFilter.getName() + ":" + "(" + filterValue.replace(",", " AND ") + ")");
        }

      }
    } else {
      typeFilterValue = TYPE_TABLE;
    }
    query.addFilterQuery("{!tag=" + TYPE + "}" + TYPE + ":" + typeFilterValue);
    query.addFacetField("{!ex=" + TYPE + "}" + TYPE);

    /* set solr search query parameter 'q' */
    String searchQueryValue = params.get(URLUtil.SEARCH_QUERY_PARAM);
    if (searchQueryValue == null || searchQueryValue.isEmpty()) {
      searchQueryValue = "*";
      query.setQuery(searchQueryValue);
    } else {
      String[] queryTerms = searchQueryValue.trim().split(" ");
      String queryTerm = "";
      for (String term : queryTerms) {
        if (term.isEmpty()) {
          continue;
        }

        if (!queryTerm.isEmpty()) {
          queryTerm += " AND ";
        }
        queryTerm += "*" + term + "*";
      }
      query.setQuery(queryTerm);
      query.setHighlight(true);
      query.setHighlightSimplePre("<b>");
      query.setHighlightSimplePost("</b>");
      query.setHighlightSnippets(100);
      query.set("hl.fl", "*");
    }

    /* set the remaining filters */
    for (SolrQueryParameter queryFilter : facetFields) {
      filter.add(queryFilter);
      String value = params.get(queryFilter.getName());

      String filterQuery = "";
      String facetField = "";
      if (queryFilter.isExcludeFromFacet()) {
        if (value != null) {
          String[] multipleFilter = value.split(",");
          value = "(";
          for (int i = 0; i < multipleFilter.length; i++) {
            String filterValue = cleanseValue(multipleFilter[i]).replace(" ", "*");
            value += "(" + filterValue + ")";
            if (i < multipleFilter.length - 1) {
              value += " " + getOperator(queryFilter.getFilterType()) + " ";
            }
          }
          value += ")";
        }
        filterQuery = "{!tag=" + queryFilter.getName() + "}" + queryFilter.getName() + ":" + value;
        facetField = "{!ex=" + queryFilter.getName() + "}" + queryFilter.getName();
      } else {
        if (value != null) {
          String[] multipleFilter = value.split(",");
          value = "(";
          for (int i = 0; i < multipleFilter.length; i++) {
            String filterValue = cleanseValue(multipleFilter[i]).replace(" ", "*");
            value += "(" + filterValue + ")";
            if (i < multipleFilter.length - 1) {
              value += " " + getOperator(queryFilter.getFilterType()) + " ";
            }
          }
          value += ")";
        }
        filterQuery = queryFilter.getName() + ":" + value;
        facetField = queryFilter.getName();
      }

      if (value != null && !value.isEmpty()) {
        query.addFilterQuery(filterQuery);
      }
      query.addFacetField(facetField);

      if (queryFilter.getFacetSort().equals(FacetSort.INDEX)) {
        query.add("f." + queryFilter.getName() + ".facet.sort", "index");
      }
      query.add("f." + queryFilter.getName() + ".facet.limit", "-1");
    }

    /* set facet queries */
    Map<String, String> facetQueryMap = new HashMap<String, String>();
    long now = System.currentTimeMillis() / 1000;
    for (SolrFacetQuery solrFacetQuery : facetQueries) {
      for (SolrHourRange range : solrFacetQuery.getRanges()) {
        long from = range.getFrom() == Long.MAX_VALUE ? 0 : now - (range.getFrom() * 3600);
        String facetQueryString = solrFacetQuery.getName() + ":[" + from + " TO " + now + "]";
        query.addFacetQuery("{!ex=" + solrFacetQuery.getName() + "}" + facetQueryString);
        facetQueryMap.put(solrFacetQuery.getName() + range.getName(), facetQueryString);
      }
      String value = params.get(solrFacetQuery.getName());
      if (value != null) {
        String fq = "{!tag=" + solrFacetQuery.getName() + "}" + facetQueryMap.get(solrFacetQuery.getName() + value);
        query.addFilterQuery(fq);
      }
    }

    /* always sort the entities (for a deterministic view) */
    query.setSort(ID, ORDER.asc);

    /* set pagination information */
    int page = getPageParameter(params);
    int elements = getElementsParameter(params);
    query.setRows(elements);
    query.setStart((page - 1) * elements);

    /* execute the query */
    QueryResponse queryResponse = null;
    try {
      queryResponse = solrClient.query(query);
    } catch (Exception e) {
      e.printStackTrace();
    }

    SolrDocumentList list = queryResponse.getResults();

    /* get table / view entities from local repository */
    List<SolrQueryResultEntity> resultEntities = new LinkedList<SolrQueryResultEntity>();
    String resultType = "";
    for (SolrDocument solrDocument : list) {
      String id = (String) solrDocument.get(ID);

      if (typeFilterValue.equalsIgnoreCase(TYPE_PARTITION)) {
        if (!searchQueryValue.equals("*")) {
          resultEntities.add(new SolrQueryResultEntity(metascopeViewService.findByViewId(id), queryResponse
                  .getHighlighting().get(id)));
        } else {
          resultEntities.add(new SolrQueryResultEntity(metascopeViewService.findByViewId(id)));
        }
        resultType = TYPE_PARTITION;
      } else if (typeFilterValue.equalsIgnoreCase(TYPE_TABLE)) {
        if (!searchQueryValue.equals("*")) {
          resultEntities.add(new SolrQueryResultEntity(metascopeTableService.findByFqdn(id), queryResponse
                  .getHighlighting().get(id)));
        } else {
          resultEntities.add(new SolrQueryResultEntity(metascopeTableService.findByFqdn(id)));
        }
      }
    }
    if (resultType.isEmpty()) {
      resultType = TYPE_TABLE;
    }

    filter.addAll(partitionFilter);

    /* get the facet values and counts */
    Map<String, List<SolrFacet>> facetValues = new HashMap<String, List<SolrFacet>>();
    for (SolrQueryParameter f : filter) {
      if (!f.getName().equals(URLUtil.SEARCH_QUERY_PARAM)) {
        List<SolrFacet> values = new ArrayList<SolrFacet>();
        FacetField facet = queryResponse.getFacetField(f.getName());
        for (Count count : facet.getValues()) {
          values.add(new SolrFacet(count.getName(), count.getCount()));
        }
        facetValues.put(f.getName(), values);
      }
    }

    /* remove the type filter */
    filter.remove(0);

    for (SolrFacetQuery solrFacetQuery : facetQueries) {
      filter
              .add(new SolrQueryParameter(solrFacetQuery.getDisplayName(), solrFacetQuery.getName(), FilterType.EXCLUSIVE));
      List<SolrFacet> values = new ArrayList<SolrFacet>();
      for (SolrHourRange range : solrFacetQuery.getRanges()) {
        long facetQueryCount = getFacetQueryCount(queryResponse, "{!ex=" + solrFacetQuery.getName() + "}"
                + facetQueryMap.get(solrFacetQuery.getName() + range.getName()));
        values.add(new SolrFacet(range.getName(), facetQueryCount));
      }
      facetValues.put(solrFacetQuery.getName(), values);
    }

    /* get the active filter values which have been selected by the user */
    addToActiveFilterValues(activeFilterValues, params, filter);

        /* is a search query */
    boolean isSearchQuery = (searchQueryValue != null && !searchQueryValue.isEmpty() && !searchQueryValue.equals("*"))
            || hasActiveFilters(activeFilterValues);


    /* build and return the result */
    SolrQueryResult result = new SolrQueryResult().withResultEntities(resultEntities).withResultType(resultType)
            .withFilters(filter).withFacetValues(facetValues).withActiveFilterValues(activeFilterValues)
            .withSearchQuery(searchQueryValue).withPage(page).withElements(elements)
            .withTotalPages((int) Math.ceil(((double) list.getNumFound()) / elements)).withTotalResults(list.getNumFound())
            .withIsSearchRequest(isSearchQuery);
    return result;
  }

  private boolean hasActiveFilters(Map<String, List<String>> activeFilterValues) {
    for (List<String> activeFilters : activeFilterValues.values()) {
      if (activeFilters.size() > 0) {
        return true;
      }
    }
    return false;
  }

  private String cleanseValue(String value) {
    String result = value;
    if (result.endsWith("_s")) {
      result = replaceLast(value, "_s", "");
    }
    return value.replace("+", " ");
  }

  private String replaceLast(String string, String toReplace, String replacement) {
    int pos = string.lastIndexOf(toReplace);
    if (pos > -1) {
      return string.substring(0, pos) + replacement + string.substring(pos + toReplace.length(), string.length());
    } else {
      return string;
    }
  }

  private long getFacetQueryCount(QueryResponse queryResponse, String key) {
    for (Entry<String, Integer> e : queryResponse.getFacetQuery().entrySet()) {
      if (e.getKey().equals(key)) {
        return e.getValue();
      }
    }
    return 0;
  }

  private String getOperator(FilterType filterType) {
    switch (filterType) {
      case AND:
        return "AND";
      case OR:
      case EXCLUSIVE:
      default:
        return "OR";
    }
  }

  private void addToActiveFilterValues(Map<String, List<String>> activeFilters, Map<String, String> params,
                                       List<SolrQueryParameter> filter) {
    for (SolrQueryParameter f : filter) {
      String paramValue = params.get(f.getName()) != null ? params.get(f.getName()) : "";
      String[] values = paramValue.split(",");
      List<String> filterValues = new ArrayList<String>();
      for (String v : values) {
        if (!v.isEmpty()) {
          filterValues.add(cleanseValue(v));
        }
      }
      activeFilters.put(f.getName(), filterValues);
    }
  }

  private int getPageParameter(Map<String, String> params) {
    String pageParam = params.get(URLUtil.PAGINATION_PAGE_PARAM);
    int page = URLUtil.PAGINATION_PAGE_DEFAULT;
    if (pageParam != null) {
      try {
        page = Integer.parseInt(pageParam);
      } catch (NumberFormatException e) {
        // ignore corrupt user input
      }
    }
    return page;
  }

  private int getElementsParameter(Map<String, String> params) {
    String elementsParam = params.get(URLUtil.PAGINATION_ELEMENTS_PARAM);
    int elements = URLUtil.PAGINATION_ELEMENTS_DEFAULT;
    if (elementsParam != null) {
      try {
        elements = Integer.parseInt(elementsParam);
      } catch (NumberFormatException e) {
        // ignore corrupt user input
      }
    }
    return elements;
  }

}
