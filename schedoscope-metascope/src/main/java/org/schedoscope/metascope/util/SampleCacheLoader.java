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
package org.schedoscope.metascope.util;

import com.google.common.cache.CacheLoader;
import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.schedoscope.metascope.util.model.HiveQueryResult;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SampleCacheLoader extends CacheLoader<String, HiveQueryResult> {

  private MetascopeTableService metascopeTableService;
  private HiveQueryExecutor hiveQueryExecutor;

  public SampleCacheLoader(MetascopeTableService metascopeTableService, HiveQueryExecutor hiveQueryExecutor) {
    this.metascopeTableService = metascopeTableService;
    this.hiveQueryExecutor = hiveQueryExecutor;
  }

  @Override
  @Transactional
  public HiveQueryResult load(String fqdn) throws Exception {
    return getSample(fqdn);
  }

  public HiveQueryResult getSample(String fqdn) {
    MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
    if (table == null) {
      return new HiveQueryResult("Internal error");
    }
    Map<String, String> params = new HashMap<String, String>();
    Set<MetascopeField> parameters = table.getParameters();
    if (parameters.size() > 0) {
      MetascopeField parameter = parameters.iterator().next();
      String parameterValue = metascopeTableService.getRandomParameterValue(table, parameter);
      params.put(parameter.getFieldName(), parameterValue);
    }
    HiveQueryResult queryResult = hiveQueryExecutor.executeQuery(table.getDatabaseName(), table.getTableName(),
      table.getFieldsCommaDelimited(), table.getParameters(), params);
    return queryResult;
  }

}
