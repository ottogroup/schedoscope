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
package org.schedoscope.metascope.service;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@Service
public class SchedoscopeCommandService {

	public static final String INVALIDATE_COMMAND = "invalidate";
	public static final String MATERIALIZE_COMMAND = "materialize";

	@Autowired
	private MetascopeConfig config;

	public void invalidateView(String urlPath) {
		schedoscopeRESTCall(INVALIDATE_COMMAND, urlPath);
	}

	public void materializeView(String urlPath) {
		schedoscopeRESTCall(MATERIALIZE_COMMAND, urlPath);
	}

	private void schedoscopeRESTCall(String action, String urlPath) {
		String url = "http://" + config.getSchedoscopeHost() + ":"
				+ config.getSchedoscopePort() + "/" + action + "/" + urlPath;
		WebResource webResource = Client.create().resource(url);
		webResource.accept("application/json").get(ClientResponse.class);
	}

}
