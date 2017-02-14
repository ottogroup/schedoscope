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
package org.schedoscope.metascope.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.schedoscope.metascope.exception.SchedoscopeConnectException;
import org.schedoscope.metascope.task.model.ViewStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedoscopeUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchedoscopeUtil.class);

  /**
   * Creates a ViewStatus from Schedoscope REST interface
   *
   * @param all Schedoscope flag; if set, more information is retrieved
   * @param filter filter to use
   * @param host host to schedoscope instance
   * @param port port to schedoscope instance
   * @return return schedoscope view status
   * @throws SchedoscopeConnectException
   */
  public static ViewStatus getViewStatus(boolean all, boolean dependencies, String filter, String host, int port)
          throws SchedoscopeConnectException {
    String json = getViewsAsJsonFromSchedoscope(all, dependencies, filter, host, port);
    if (json == null) {
      return null;
    }
    return getViewStatusFromJson(json);
  }

  private static ViewStatus getViewStatusFromJson(String json) {
    ViewStatus viewStatus = null;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      viewStatus = objectMapper.readValue(json, ViewStatus.class);
    } catch (Exception e) {
      LOG.error("Could not parse JSON from Schedoscope REST API (/). Aborting task execution ...", e);
    }
    return viewStatus;
  }

  /**
   * Get views list from Schedoscope REST interface
   *
   * @return the JSON output created by Schedoscopes REST interface
   * @throws SchedoscopeConnectException
   */
  private static String getViewsAsJsonFromSchedoscope(boolean all, boolean dependencies, String filter, String host, int port)
          throws SchedoscopeConnectException {
    String url = "http://" + host + ":" + port + "/views/?";
    if (all) {
      url += "&all=true";
    }
    if (dependencies) {
      url += "&dependencies=true";
    } else {
      url += "&dependencies=false";
    }
    if (filter != null) {
      url += "&filter=" + filter;
    }

    return makeRequest(url);
  }

  private static String makeRequest(String url) throws SchedoscopeConnectException {
    Client client = Client.create();
    WebResource webResource = client.resource(url);
    ClientResponse response = null;

    try {
      LOG.info("Execute request ...");
      long start = System.currentTimeMillis();
      response = webResource.accept("application/json").get(ClientResponse.class);
      LOG.info("Finished request in " + (System.currentTimeMillis() - start) + " ms");
    } catch (Exception e) {
      LOG.debug("Could not connect to Schedoscope instance", e);
    }

    if (response == null || response.getStatus() != 200) {
      LOG.error("Could not connect to Schedoscope REST API "
              + "(Schedoscope is not running or host/port information may be wrong)");
      throw new SchedoscopeConnectException("Could not connect to schedoscope", new Throwable("Could not connect"));
    }
    client.destroy();
    return response.getEntity(String.class);
  }

}
