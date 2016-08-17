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
package org.schedoscope.metascope.service;

import org.schedoscope.metascope.repository.ViewEntityRepository;
import org.schedoscope.metascope.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map.Entry;

@Service
public class URLService {

    private static final Logger LOG = LoggerFactory.getLogger(URLService.class);

    private static final String URL_PARAMS_DIVIDER = "?";
    private static final String URL_PARAMS_AND = "&";
    private static final String URL_PARAMS_EQUALS = "=";
    private static final String COMMA = ",";
    private static final String ENCODED_COMMA = "%2C";

    @Autowired
    private ViewEntityRepository viewEntityRepository;

    public String getPaginationURL(HttpServletRequest request, int page, int elements) {
        String queryParams = "";
        for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
            if (!e.getKey().equals(URLUtil.PAGINATION_PAGE_PARAM) && !e.getKey().equals(URLUtil.PAGINATION_ELEMENTS_PARAM)) {
                queryParams = addKey(queryParams, e.getKey(), e.getValue()[0]);
            }
        }
        queryParams = addKey(queryParams, URLUtil.PAGINATION_PAGE_PARAM, "" + page);
        queryParams = addKey(queryParams, URLUtil.PAGINATION_ELEMENTS_PARAM, "" + elements);
        return request.getRequestURL() + URL_PARAMS_DIVIDER + queryParams;
    }

    public String getFilterURL(HttpServletRequest request, String key, String value) {
        value = cleanseValue(value);
        String queryParams = "";
        boolean keyAlreadyExists = false;
        for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
            if (e.getKey().equals(URLUtil.PAGINATION_PAGE_PARAM)) {
                queryParams = addKey(queryParams, e.getKey(), "1");
            } else if (key.equals(e.getKey())) {
                keyAlreadyExists = true;
                String paramValue = cleanseValue(e.getValue()[0]);
                if (!value.equals(paramValue)) {
                    boolean alreadyContainsValue = false;
                    for (String v : paramValue.split(ENCODED_COMMA)) {
                        if (value.equals(v)) {
                            queryParams = removeValueFromKey(queryParams, e.getKey(), paramValue, value);
                            alreadyContainsValue = true;
                        }
                    }
                    if (!alreadyContainsValue) {
                        queryParams = addKey(queryParams, e.getKey(), paramValue + COMMA + value);
                    }
                }
            } else {
                queryParams = addKey(queryParams, e.getKey(), cleanseValue(e.getValue()[0]));
            }
        }

        if (!keyAlreadyExists) {
            queryParams = addKey(queryParams, key, value);
        }

        return request.getRequestURL() + URL_PARAMS_DIVIDER + queryParams;
    }

    public String getExclusiveFilterURL(HttpServletRequest request, String key, String value) {
        value = cleanseValue(value);
        String queryParams = "";
        boolean doNotAdd = false;
        for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
            if (e.getKey().equals(URLUtil.PAGINATION_PAGE_PARAM)) {
                queryParams = addKey(queryParams, e.getKey(), "1");
            } else if (!e.getKey().equals(key)) {
                queryParams = addKey(queryParams, e.getKey(), e.getValue()[0]);
            } else {
                if (value.equals(cleanseValue(e.getValue()[0]))) {
                    doNotAdd = true;
                }
            }
        }
        if (!doNotAdd) {
            queryParams = addKey(queryParams, key, value);
        }
        return request.getRequestURL() + URL_PARAMS_DIVIDER + queryParams;
    }

    public String removeFromFilterURL(HttpServletRequest request, String key) {
        String queryParams = "";
        for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
            if (!e.getKey().equals(key)) {
                if (!queryParams.isEmpty()) {
                    queryParams += URL_PARAMS_AND;
                }
                queryParams += e.getKey() + URL_PARAMS_EQUALS + cleanseValue(e.getValue()[0]);
            }
        }
        return request.getRequestURL() + URL_PARAMS_DIVIDER + queryParams;
    }

    public int getPartitionPage(String fqdn, String viewId) {
        int position = viewEntityRepository.getPartitionPosition(fqdn, viewId);
        return (position / 20) + 1;
    }

    private String addKey(String queryParams, String key, String value) {
        if (!queryParams.isEmpty()) {
            queryParams += URL_PARAMS_AND;
        }
        return queryParams += key + URL_PARAMS_EQUALS + value;
    }

    private String removeValueFromKey(String queryParams, String key, String values, String value) {
        String[] split = values.split(ENCODED_COMMA);
        String queryVals = "";
        for (String v : split) {
            if (!v.equals(value)) {
                if (!queryVals.isEmpty()) {
                    queryVals += COMMA;
                }
                queryVals += v;
            }
        }
        if (!queryVals.isEmpty()) {
            if (!queryParams.endsWith(URL_PARAMS_AND)) {
                queryParams += URL_PARAMS_AND;
            }
            queryParams += key + URL_PARAMS_EQUALS + queryVals;
        }
        return queryParams;
    }

    private String cleanseValue(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.warn("Could not url encode parameter value", e);
        }
        return value;
    }

}
