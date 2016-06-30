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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.service.TableEntityService;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.cache.CacheLoader;

public class SampleCacheLoader extends CacheLoader<String, HiveQueryResult> {

  private TableEntityService tableEntityService;
  private HiveQueryExecutor hiveUtil;

  public SampleCacheLoader(TableEntityService tableEntityService, HiveQueryExecutor hiveUtil) {
    this.tableEntityService = tableEntityService;
    this.hiveUtil = hiveUtil;
  }

  @Override
  @Transactional
  public HiveQueryResult load(String fqdn) throws Exception {
    return getSample(fqdn);
  }

  public HiveQueryResult getSample(String fqdn) {
    TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
    if (tableEntity == null) {
      return new HiveQueryResult("Internal error");
    }
    Map<String, String> params = new HashMap<String, String>();
    List<FieldEntity> parameters = tableEntity.getParameters();
    if (parameters.size() > 0) {
      FieldEntity parameter = parameters.get(0);
      String parameterValue = tableEntityService.getRandomParameterValue(tableEntity, parameter);
      params.put(parameter.getName(), parameterValue);
    }
    HiveQueryResult queryResult = hiveUtil.executeQuery(fqdn, tableEntity.getFieldsCommaDelimited(),
        tableEntity.getParameters(), params);
    return queryResult;
  }

}
