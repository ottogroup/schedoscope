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
package org.schedoscope.metascope.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Service
public class MetascopeStatusService {

  public static final String NOT_INITIALIZED = "unknown";

  @Autowired
  private MetascopeConfig config;

  @Autowired
  private MetascopeTableService metascopeTableService;

  @Autowired
  private MetascopeViewService metascopeViewService;

  /** sample cache */
  private Cache<String, String> statusCache;

  @PostConstruct
  public void init() {
    this.statusCache = CacheBuilder.newBuilder().maximumSize(2000000).expireAfterWrite(60, TimeUnit.SECONDS).build();
  }

  public String getStatus(String qualifier) {
    String status = statusCache.getIfPresent(qualifier);
    return status != null ? status : NOT_INITIALIZED;
  }

  public void setStatus(String qualifier, String status) {
    this.statusCache.put(qualifier, status);
  }

}
