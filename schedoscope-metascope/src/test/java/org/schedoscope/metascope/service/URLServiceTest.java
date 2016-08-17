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

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class URLServiceTest extends SpringTest {

    private static final String LOCALHOST = "http://localhost:8080/";
    private static final String SOME_URL_KEY = "someKey";
    private static final String SOME_URL_VALUE = "someValue";
    private static final String SOME_URL_PARAMETER = SOME_URL_KEY + "=" + SOME_URL_VALUE;
    private static final String FILTER_URL_KEY = "filterByKey";
    private static final String FILTER_URL_VALUE_1 = "filterByValue1";
    private static final String FILTER_URL_VALUE_2 = "filterByValue2";
    private static final String FILTER_URL_PARAMETER = FILTER_URL_KEY + "=" + FILTER_URL_VALUE_1;
    private static final String PAGINATION_URL_PART = "p=0&e=10";

    private HttpServletRequest request;

    @Before
    public void setupLocal() {
        this.request = mock(HttpServletRequest.class);
        when(request.getRequestURL()).thenReturn(new StringBuffer(LOCALHOST));
    }

    @Test
    public void urlService_01_getPaginationUrl() {
        String url = urlService.getPaginationURL(request, 0, 10);

        assertEquals(url, LOCALHOST + "?" + PAGINATION_URL_PART);
    }

    @Test
    public void urlService_02_getPaginationUrlWithParams() {
        Map<String, String[]> urlParams = new HashMap<String, String[]>();
        urlParams.put(SOME_URL_KEY, new String[]{SOME_URL_VALUE});
        when(request.getParameterMap()).thenReturn(urlParams);

        String url = urlService.getPaginationURL(request, 0, 10);

        assertEquals(url, LOCALHOST + "?" + SOME_URL_PARAMETER + "&" + PAGINATION_URL_PART);
    }

    @Test
    public void urlService_03_getFilterUrlClean() {
        String url = urlService.getFilterURL(request, FILTER_URL_KEY, FILTER_URL_VALUE_1);

        assertEquals(url, LOCALHOST + "?" + FILTER_URL_PARAMETER);
    }

    @Test
    public void urlService_04_getFilterWithOtherParameter() {
        Map<String, String[]> urlParams = new HashMap<String, String[]>();
        urlParams.put(SOME_URL_KEY, new String[]{SOME_URL_VALUE});
        when(request.getParameterMap()).thenReturn(urlParams);

        String url = urlService.getFilterURL(request, FILTER_URL_KEY, FILTER_URL_VALUE_1);

        assertEquals(url, LOCALHOST + "?" + SOME_URL_PARAMETER + "&" + FILTER_URL_PARAMETER);
    }

    @Test
    public void urlService_05_getFilterWithSameParameter() {
        Map<String, String[]> urlParams = new HashMap<String, String[]>();
        urlParams.put(FILTER_URL_KEY, new String[]{FILTER_URL_VALUE_1});
        when(request.getParameterMap()).thenReturn(urlParams);

        String url = urlService.getFilterURL(request, FILTER_URL_KEY, FILTER_URL_VALUE_2);

        assertEquals(url, LOCALHOST + "?" + FILTER_URL_PARAMETER + "," + FILTER_URL_VALUE_2);
    }

    @Test
    public void urlService_06_getExclusiveFilter() {
        Map<String, String[]> urlParams = new HashMap<String, String[]>();
        urlParams.put(FILTER_URL_KEY, new String[]{FILTER_URL_VALUE_2});
        when(request.getParameterMap()).thenReturn(urlParams);

        String url = urlService.getExclusiveFilterURL(request, FILTER_URL_KEY, FILTER_URL_VALUE_1);

        assertEquals(url, LOCALHOST + "?" + FILTER_URL_PARAMETER);
    }

    @Test
    public void urlService_07_removeFromFilter() {
        Map<String, String[]> urlParams = new HashMap<String, String[]>();
        urlParams.put(FILTER_URL_KEY, new String[]{FILTER_URL_VALUE_2});
        when(request.getParameterMap()).thenReturn(urlParams);

        String url = urlService.removeFromFilterURL(request, FILTER_URL_KEY);

        assertEquals(url, LOCALHOST + "?");
    }

}
