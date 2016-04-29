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
package org.schedoscope.metascope.util;

import java.util.ArrayList;
import java.util.List;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.schedoscope.model.View;
import org.schedoscope.metascope.schedoscope.model.ViewStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@Component
public class SchedoscopeUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchedoscopeUtil.class);

  public static final String FAILED_STATUS = "failed";
  public static final String WAITING_STATUS = "waiting";
  public static final String TRANSFORMING_STATUS = "transforming";
  public static final String INVALIDATED_STATUS = "invalidated";
  public static final String NODATA_STATUS = "nodata";
  public static final String RETRYING_STATUS = "retrying";
  public static final String RECEIVE_STATUS = "receive";
  public static final String MATERIALIZED_STATUS = "materialized";

  @Autowired
  private MetascopeConfig config;
  private ObjectMapper objectMapper;

  public SchedoscopeUtil() {
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Get the aggregated status for tables from their views
   * 
   * @param views
   *          all views belonging to a table
   * @return
   */
  public String getStatus(List<ViewEntity> views) {
    boolean failed = false;
    boolean nodata = false;
    boolean transforming = false;
    boolean waiting = false;
    boolean invalidated = false;
    boolean receive = false;
    boolean retrying = false;
    for (ViewEntity viewEntity : views) {
      if (viewEntity.getStatus().equals(FAILED_STATUS)) {
        failed = true;
        break;
      } else if (viewEntity.getStatus().equals(WAITING_STATUS)) {
        waiting = true;
      } else if (viewEntity.getStatus().equals(TRANSFORMING_STATUS)) {
        transforming = true;
      } else if (viewEntity.getStatus().equals(INVALIDATED_STATUS)) {
        invalidated = true;
      } else if (viewEntity.getStatus().equals(NODATA_STATUS)) {
        nodata = true;
      } else if (viewEntity.getStatus().equals(RECEIVE_STATUS)) {
        receive = true;
      } else if (viewEntity.getStatus().equals(RETRYING_STATUS)) {
        retrying = true;
      }
    }

    if (failed) {
      return FAILED_STATUS;
    }
    if (transforming) {
      return TRANSFORMING_STATUS;
    }
    if (retrying) {
      return RETRYING_STATUS;
    }
    if (receive) {
      return RECEIVE_STATUS;
    }
    if (waiting) {
      return WAITING_STATUS;
    }
    if (invalidated) {
      return INVALIDATED_STATUS;
    }
    if (nodata) {
      return NODATA_STATUS;
    }
    return MATERIALIZED_STATUS;
  }

  /**
   * Creates a list of views from a ViewStatus created by Schedoscope REST
   * interface
   * 
   * @param all
   *          Schedoscope flag; if set, more information is retrieved
   * @return
   */
  public List<ViewEntity> getViews(boolean all) {
    List<ViewEntity> views = new ArrayList<ViewEntity>();
    ViewStatus viewStatus = getViewStatus(all);
    if (viewStatus != null) {
      for (View view : viewStatus.getViews()) {
        ViewEntity viewEntity = new ViewEntity();
        viewEntity.setUrlPath(view.getName());
        viewEntity.setStatus(view.getStatus());
        views.add(viewEntity);
      }
    }
    return views;
  }

  /**
   * Creates a ViewStatus from Schedoscope REST interface
   * 
   * @param all
   *          Schedoscope flag; if set, more information is retrieved
   * @return
   */
  public ViewStatus getViewStatus(boolean all) {
    String json = getViewsAsJsonFromSchedoscope(all);
    if (json == null) {
      return null;
    }
    return getViewStatusFromJson(json);
  }

  public ViewStatus getViewStatusFromJson(String json) {
    ViewStatus viewStatus = null;
    try {
      if (objectMapper == null) {
        objectMapper = new ObjectMapper();
      }
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
   */
  public String getViewsAsJsonFromSchedoscope(boolean all) {
    Client client = Client.create();
    String url = "http://" + config.getSchedoscopeHost() + ":" + config.getSchedoscopePort() + "/";
    if (all) {
      url += "?all=true";
    }
    WebResource webResource = client.resource(url);
    ClientResponse response;

    try {
      response = webResource.accept("application/json").get(ClientResponse.class);
    } catch (Exception e) {
      LOG.info("Could not connect to Schedoscope REST API", e);
      return null;
    }

    if (response.getStatus() != 200) {
      LOG.error("Could not connect to Schedoscope REST API "
          + "(Schedopscope is not running or host/port information may be wrong");
      return null;
    }
    client.destroy();
    return response.getEntity(String.class);
  }

}
